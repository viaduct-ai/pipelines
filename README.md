# pipelines


[![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=viaduct-ai/pipelines)](https://dependabot.com) [![DepShield Badge](https://depshield.sonatype.org/badges/viaduct-ai/pipelines/depshield.svg)](https://depshield.github.io) ![Lint and Test](https://github.com/viaduct-ai/pipelines/workflows/Lint%20and%20Test/badge.svg)

# Background 
`pipelines` was created as simple interface for developing and modifying CI pipelines at [Viaduct](https://www.viaduct.ai). Its a lightweight wrapper on Go channels for specifying dependencies between, running, and gracefully shutting down asynchronous processes. `pipelines` gives developers a intuitive way to declared dependencies amongst concurrent processes to create a **pipeline** (DAGs) and a structured way to add new or modify existing functionality.

A pipeline could be triggered by a webhook, or on an interval via a `TickerProcess`. 

# Overview

## Example

```go
package main

import "github.com/viaduct-ai/pipelines"

pipeline := pipelines.New()

// todo
```

### Filtering
### Error Handling
