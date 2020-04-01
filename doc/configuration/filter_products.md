# Filter Products

Configuration is available in Magento 2 back-office under:
* Stores > Catalog > Pimgento > Filter products

| Configuration                 | Usage                                                                      |
|-------------------------------|----------------------------------------------------------------------------|
| Mode                          | Choose your method to filter product data: Standard or Advanced           |

### Advanced

| Configuration                 | Usage                                                                      |
|-------------------------------|----------------------------------------------------------------------------|
| Advanced Filter               | Build your query using a JSON                                              |

### Standard

| Configuration                 | Usage                                                                      |
|-------------------------------|----------------------------------------------------------------------------|
| Completeness type             | Choose your logical operator ex (Equals, Lower than...)                    |
| Channel                       | Select the channel to apply the completeness filter on                     |
| Completeness value            | Fix the completeness level required for import                             |
| Status                        | Choose the status of the products to import (Enabled, Disabled, No condition)|
| Updated                       | Import products updated since last X days                                  |
| Families                      | Select the families you don't want to retrieve products from               |