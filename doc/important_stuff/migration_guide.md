# How to migrate from PIMGento2 CSV version to API version

### First of, you will need to disable all PIMGento2 modules:
```bash
php bin/magento module:disable Pimgento_Category
php bin/magento module:disable Pimgento_Family
php bin/magento module:disable Pimgento_Attribute
php bin/magento module:disable Pimgento_Option
php bin/magento module:disable Pimgento_Variant
php bin/magento module:disable Pimgento_Product

php bin/magento module:disable Pimgento_Import
php bin/magento module:disable Pimgento_Entities

php bin/magento module:disable Pimgento_Log
```

### Then, flush cache:
```bash
php bin/magento cache:flush
```

### Install new PIMGento2 (API) Extension:
```bash
composer require agencednd/module-pimgento-2-api

php bin/magento module:enable Pimgento_Api
```

### Check and update database setup:
```bash
php bin/magento setup:db:status
php bin/magento setup:upgrade
```

### Flush cache again:
```bash
php bin/magento cache:flush
```

### Configure PIMGento2 (API) in Magento 2 backend:
Please, follow our configuration guide: [III. Configuration](../important_stuff/how_to.md)

### Re-import all entities:
Please, follow our configuration guide: [III. Configuration](../important_stuff/how_to.md)

### Important notes:
* It is important to know that the table named "pimgento_variant" in the CSV version is now called "pimgento_product_model", this is why you need to re-import all entities after migrating to be sure that all data will be set correctly.
* All custom rewrites of the previous PIMGento2 extension will be obsolete when migrating to the API version.