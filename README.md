# Transparency Engine

Transparency Engine aims to detect and communicate the implicit structure of complex activities in real-world problem areas, in ways that support both situational awareness and targeted action. A potential application is to identify and counter adversarial activities (e.g., corruption) hidden within large-scale datasets.

Given a collection of streaming data sources describing the attributes and actions of real-world entities, it uses a range of graph modeling, joint graph embedding, and other statistical techniques to detect and explain the networks of entities most closely related to each entity. 

To prioritize the expert review of these entity networks, entities can be linked to "review flags" that indicate the need for inspection. Review flags may be signs of either opportunities ("green flags") or risks ("red flags") thought to transfer between closely-related entities. The more review flags in an entity network, the higher the priority of that network for review.

For each entity in the dataset, Transparency Engine generates a narrative report illustrating the detected relationships and review flags contributing to its overall review priority. In typical use, review of high-priority reports will help inform real-world actions targeted at either the entity or its broader network. 

Transparency Engine consists of three components:
- Pipeline: Python package that uses graph modeling techniques to detect networks of closely-related entities.
- API: FastAPI interface that supports querying of the entity report using outputs produced by the entity relationship modeling pipeline.
- Report: React-based application that enables viewing of the entity narrative report and exporting the report to a PDF file.


## Getting Started

To run the pipeline locally, ensure that you have Docker installed and running on your machine. You can find instructions for installing Docker [here](https://docs.docker.com/engine/install/),

### Pipeline

To configure the pipeline, specify the pipeline configuration using two json files:
- Pipeline config: Specify steps to be executed in the pipeline. An example pipeline config can be found in `samples/config/pipeline.json`
- Steps config: Specify configurations for each step of the pipeline. An example steps config can be found in `samples/config/steps.json`.

To launch the pipeline's Docker container, execute the following command from the root of the project:
```bash
docker build Dockerfile -t transparency-engine
docker run -it transparency-engine
```

To run the pipeline, once in Docker interactive mode, execute the following command from the `python/transparency-engine` folder:
```bash
poetry run python transparency_engine/main.py --config pipeline.json --steps steps.json
```

The pipeline can also be launched in Visual Studio Code. To do so, open the project in Visual Studio Code and attach the project to a Docker container by following the instructions [here](https://code.visualstudio.com/docs/remote/containers).

The pipeline can also be packaged as a wheel file to be installed on Azure Synapse or Databricks. To create the wheel file, execute the following command from the `python/transparency-engine` folder:
```bash
poetry build
```

### API

To install the dependencies needed for the API, execute the following commands from the `python/api-backend` folder:
```bash
pip install poetry
poetry install
```

To run the backend API execute from the root of the project:
```bash
docker-compose up backend_api --build
```

### Report

To run the UI, you can either use `docker-compose` or install node and yarn and execute the following commands from the root of the project:
```bash
yarn
yarn build
yarn start # run the webapp locally
```
The webapp will be available at http://localhost:3000"


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

