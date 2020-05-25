# pipelines


[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=viaduct-ai/pipelines)](https://dependabot.com) [![DepShield Badge](https://depshield.sonatype.org/badges/viaduct-ai/pipelines/depshield.svg)](https://depshield.github.io) ![Lint and Test](https://github.com/viaduct-ai/pipelines/workflows/Lint%20and%20Test/badge.svg)

# Background
`pipelines` was created as simple interface for developing and modifying CI pipelines at [Viaduct](https://www.viaduct.ai). Its a lightweight wrapper on Go channels for specifying dependencies between, running, and gracefully shutting down asynchronous processes. `pipelines` gives developers a intuitive way to declared dependencies amongst concurrent processes to create a **pipeline** (DAGs) and a structured way to add new or modify existing functionality.

# Overview

## Example

Below is an example pipeline that polls the Github Status API every minute, filters for statuses that indicate degregaded services, then notifies the engineering team via Slack and email.
```go
package main

import (
  "encoding/json"
  "net/http"
  "net/smtp"
  "time"

  "github.com/viaduct-ai/pipelines/processor"
  "github.com/viaduct-ai/pipelines"
)

const (
  githubStatusAPI = "https://kctbh9vrtdwd.statuspage.io/api/v2/status.json"
  // fake webhook
  slackWebhookURL = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
)

type GithubStatusAPIResp struct {
  Status GithubStatus `json:"status"`
}
type GithubStatus struct {
  Description string `json:"description"`
  Indicator string `json:"indicator"`
}

func main() {
  minuteTicker, err := processor.NewTicker(time.Minute)
  getGithuStatusProc, err := processor.New(getGithubStatus, nil)
  githubOutages, err := processor.New(filterForOutages, nil)
  slackAlertProc, err := processor.New(slackAlert, nil)
  emailAlertProc, err := processor.New(emailAlert, nil)

  pipeline := pipelines.New()

  // Get Github's status every minute
  pipeline.Process(getGithubStatusProc).Consumes(minuteTicker)
  // Check Github status for outages
  pipeline.Process(githubOutages).Consumes(getGithubStatusProc)
  // Slack & Email alerts consume Github outage events
  pipeline.Processes(slackAlert, emailAlert).Consumes(githubOutages)

  // Start the pipeline
  pipeline.Run()

  // Wait for a termination signal
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  <-sigs

  // Gracefully shutdown all processes in the Pipeline
  pipeline.Shutdown()
}

func getGithubStatus(i interface{}) (interface{}, error) {
  var result GithubStatusAPIResp

  resp, err := http.Get(githubStatusAPI)

  if err != nil {
    return result, error
  }
  defer resp.Body.Close()

  json.NewDecoder(resp.Body).Decode(&result)

  return interface{}(result)
}

func filterForOutages(i interface{}) (interface{}, error) {
  switch v := i.(type) {
    case GithubStatusAPIResp:
      // none, minor, major, or critical
      if indicator := v.Status.Indicator; indicator == "none" {
        return nil, nil
      }
      return i, nil
    default:
      return nil, nil
  }
}

type SlackRequestBody struct {
    Text string `json:"text"`
}


func slackAlert(i interface{}) (interface{}, error) {
  msg := "Heads up! Github is experience issues."
  slackBody, _ := json.Marshal(SlackRequestBody{Text: msg})
  req, err := http.NewRequest(http.MethodPost, webhookUrl, bytes.NewBuffer(slackBody))
  if err != nil {

      return err
  }

  req.Header.Add("Content-Type", "application/json")

  client := &http.Client{Timeout: 10 * time.Second}
  resp, err := client.Do(req)
  if err != nil {
      return nil, err
  }

  buf := new(bytes.Buffer)
  buf.ReadFrom(resp.Body)
  if buf.String() != "ok" {
      return nil, errors.New("Non-ok response returned from Slack")
  }

  return nil, nil
}

func emailAlert(i interface{})(interface{}, error){
	// Choose auth method and set it up
  auth := smtp.PlainAuth(
		"",
		"user@example.com",
		"password",
		"mail.example.com",
	)

  to := []string{"engineering@viaduct.ai"}
  msg := []byte(
	  "Subject: Expect Issues with Github\r\n" +
	  "Github is down. Go home.")
  err := smtp.SendMail("mail.example.com:25", auth, "sender@example.org", to, msg)

  return nil, err
}


```

### Filtering
A `nil` value returned from `Processor.Process` will be implicitly filtered and not sent to any of its consumers. For example, if you had many `Processor`s that only care about a Github push events, then you could create a upstream `Processor` that consumes all Github events but returns `nil` for any non push event.


### Error Handling
Currently, all errors returned by `Processor.Process` will be ignored and logged to `stderr`. This is subject to change in the future.
