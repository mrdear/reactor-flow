# Reactor Flow Spring Boot Example

This project demonstrates how to use the Reactor Flow engine with Spring Boot. It provides a REST API for creating and executing flows.

## Overview

The Reactor Flow engine is a framework for executing directed acyclic graphs (DAGs) of tasks. This example shows how to integrate the engine with Spring Boot and expose it via a REST API.

## Features

- Spring Boot integration
- REST API for executing flows
- Support for different task types (START, FAST, DELAY, AGENT, END)
- Flow execution tracking with timing information

## Getting Started

### Prerequisites

- Java 8 or higher
- Maven 3.6 or higher

### Building the Project

```bash
mvn clean install
```

### Running the Application

```bash
mvn spring-boot:run
```

The application will start on port 8080 by default.

## API Endpoints

### Health Check

```
GET /api/flow/health
```

Returns the health status of the flow engine.

Example response:
```json
{
  "status": "UP",
  "message": "Flow engine is running"
}
```

### Execute Simple Flow

```
GET /api/flow/execute-simple
```

Executes a simple flow with START -> FAST -> END nodes.

Example response:
```json
{
  "status": "success",
  "flowId": "550e8400-e29b-41d4-a716-446655440000",
  "duration": "123 ms",
  "message": "Flow executed successfully"
}
```

### Execute Agent Flow

```
GET /api/flow/execute-agent?prompt=hello%20agent
```

Executes a simple agent flow with START -> AGENT -> END nodes.

Example response:
```json
{
  "status": "completed",
  "flowId": "550e8400-e29b-41d4-a716-446655440000",
  "prompt": "hello agent",
  "agentResult": "demo-agent final: echo_prompt => HELLO AGENT",
  "agentHistorySize": 4
}
```

## Project Structure

- `src/main/java/fun/libx/flow/mvc/Application.java`: Spring Boot application entry point
- `src/main/java/fun/libx/flow/mvc/config/FlowEngineConfig.java`: Configuration for the flow engine
- `src/main/java/fun/libx/flow/mvc/controller/FlowController.java`: REST controller for the flow engine
- `src/main/java/fun/libx/flow/mvc/model/ExtendedFlowContext.java`: Extended flow context with additional fields
- `src/main/java/fun/libx/flow/mvc/DefaultFlowTaskEngineRouter.java`: Router for task instances

## Task Types

The following task types are supported:

- `START`: Starting point of a flow
- `FAST`: A task that executes quickly
- `DELAY`: A task that has some delay (HTTP delay in this example)
- `AGENT`: A task that runs the built-in agent loop and tools
- `END`: Ending point of a flow

## Extending the Application

To add new task types:

1. Create a new implementation of `FlowTaskInstance`
2. Add the new task type to the `TaskType` enum
3. Update the `DefaultFlowTaskEngineRouter` to handle the new task type

## License

This project is licensed under the MIT License - see the LICENSE file for details.
