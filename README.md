# Eth2 Crawler
Eth2 Crawler is Ethereum blockchain project that extracts eth2 node information from the network save it to the datastore. It also exposes a graphQL interface to access the information saved in the datastore.

## Getting Started
There are three main components in the project:
1. Crawler: crawls the network for eth2 nodes, extract additional information about the node and save it to the datastore
2. MongoDB: datastore to save eth2 nodes information
3. GraphQL Interface: provide access the stored information

### Prerequisites
* docker
* docker-compose

### Environment Setup
Before building, please make sure environment variables `RESOLVER_API_KEY`(which is used to fetch information about node using IP) is setup properly. You can get your key from [IP data dashboard](https://dashboard.ipdata.co). To setup the variable, create an `.env` in the same folder as of `docker-compose.yaml`

Example `.env` File
```shell
RESOLVER_API_KEY=your_ip_data_key
```

### Configs and Flags
Eth2 crawler support config through yaml files. Default yaml config is provided at `cmd/config/config.dev.yaml`. You can use your own config file by providing it's path using the `-p` flag 

### Usage
We use docker-compose for testing locally. Once you have defined the environment variable in the `.env` file, you can start the server using:
```shell
make run
```

## Additional Commands
 * `make run`  - run the crawler service
 * `make lint` - run linter
 * `make test` - runs the test cases
 * `make license` - add license to the missing files
 * `make license-check` - checks for missing license headers

## LICENSE
See the [LICENSE](https://github.com/ChainSafe/eth2-crawler/blob/main/LICENSE) file for license rights and limitations (lgpl-3.0).
