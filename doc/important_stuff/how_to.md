# How to...

### Q: How to install PIMGento2 (API)?
**A**: You can install PIMGento2 (API) with composer:

Install module by Composer as follows:

```bash
composer require agencednd/module-pimgento-2-api
```

Enable and install module in Magento:

```bash
php bin/magento module:enable Pimgento_Api
```

Check and update database setup:
```bash
php bin/magento setup:db:status
php bin/magento setup:upgrade
```

Flush Magento caches
```bash
php bin/magento cache:flush
```

### Q: How to configure PIMGento2 (API)?
**A**: Before starting to use PIMGento2 (API), few steps are require to set it right:
* Configure your store language and currency before import
* Configure your API settings in "Store > Configuration > Catalog > Pimgento"
* Launch import from admin panel in "System > Pimgento > Import"
* After category import, set the "Root Category" for store in "Stores > Settings > All Stores"
...and you are good to go! Just check the configuration to be ready to import your data the right way!

### Q: How to import my data into PIMGento2 (API)?
**A**: You can import your data using two different ways:
* Using the [interface](../features/pimgento_interface.md)
* Using [cron tasks](../features/pimgento_cron.md)

But before using one of these methods be sure to read this [quick guide](../features/pimgento_import.md) about the import system.

### Q: How to customize PIMGento2 (API)?
**A**: If even the multiple configuration of PIMGento2 (API) doesn't suit your business logic, or if you want to have other possibilities in import, you can always override PIMGento2 (API) as it is completely Open Source. Just keep in mind a few things before beginning to develop your own logic:
* Observers define each task for a given import, if you want to add a task you should declaring a new method in the corresponding Import class and adding to the Observer.
* One method in Import class = One task
* There is no data transfer between tasks

Note that if you judge your feature can be used by others, and if you respect this logic, we will be glad to add it to PIMGento2 (API): just make us a PR!

### Q: How to contribute to PIMGento2 (API)?
**A**: You can contribute to PIMGento2 (API) by submitting PR on Github. However, you need to respect a few criteria:
* Respect PIMGento2 (API) logic and architecture
* Be sure to not break others features
* Submit a clean code
* Always update the documentation if you submit a new feature

### Q: How to set product name as URL
**A**: Add pim attribute mapping that points to the field "url_key"
* Go to Store -> Configuration -> Catalog -> PIMGento2
* Add attribute mapping
* Set Pim to "akeneo_product_name_field"
* Set Magento to "url_key"
* Save