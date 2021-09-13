# Installation

- [Installation](#installation)
  - [Pre-requisites](#pre-requisites)
    - [Docker](#docker)
    - [Pre-commit](#pre-commit)
  - [Installation steps](#installation-steps)

## Pre-requisites
### Pre-commit
This repo utilizes the power of pre-commit to identify simple programming issues at the time of code check-in. This helps the reviewer to focus more on architectural and conceptual issues and reduce the overall time to market.

The pre-commit configurations are stored in [.pre-commit-config.yaml](../.pre-commit-config.yaml) file.

To know about the syntax, visit the [official documentation site](https://pre-commit.com/).

The pre-commit hooks in this repository are 
    - flake8:  flake8 is a command-line utility for enforcing style consistency across Python projects.
    - autopep8: autopep8 automatically formats Python code to conform to the PEP 8 style guide.

1. Install pre-commit
    Use `pip` to install pre-commit
    ```sh
    pip install pre-commit
    ```

    Or using homebrew on macOS
    ```sh
    brew install pre-commit
    ```

    For more installation alternatives, check out [Pre-commit official documentation](https://pre-commit.com/#install).
2. Verify pre-commit installation
    ```sh
    pre-commit --version