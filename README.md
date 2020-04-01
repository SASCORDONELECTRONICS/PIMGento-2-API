![PIMGento2](doc/pimgento2-api-logo.png)

This fork was created to adapt Pimgento2 to the needs of Cordon Electronices. The goal is to allow Magento configurables to be imported not necessarily from the parents of PIM products, but if need be from their grandparents.

# PIMGento2 (API)

PIMGento2 (API) is a Magento 2 extension that allows you to import your catalog from Akeneo API into Magento.

You can discover PIMGento2 (API) on the official website (https://www.pimgento.com/).

> The first version of the PIMGento2 connector will not be maintained anymore by the end of 2018 (https://github.com/Agence-DnD/PIMGento-2).

### Documentation

PIMGento2 (API) complete documentation is available [here](doc/summary.md).
Akeneo API complete documentation is available [here](https://api.akeneo.com/).

### How it works

PIMGento2 (API) fetches data from Akeneo API and insert data directly in Magento database.

In this way, it makes imports very fast and doesn't disturb your e-commerce website.

With PIMGento2 (API), you can import :
* Categories
* Families
* Attributes
* Options
* Product Model (Akeneo >= 2.0)
* Family Variant (Akeneo >= 2.0)
* Products
* Assets (Magento EE & Akeneo >= 2.0)

### Requirements

* Akeneo PIM >= 2.0 (CE & EE)
* Magento >= 2.1 (CE & EE)
* Database encoding must be UTF-8

### Installation, Configuration and Usage

If you want to know how to install, configure or use PIMGento2 (API), please check [how to...](doc/important_stuff/how_to.md) section. We advise you to start here!
If you want to migrate from the PIMGento2 CSV connector to this one, please follow our [migration guide](doc/important_stuff/migration_guide.md).

### Roadmap

You can consult our roadmap [here](doc/important_stuff/roadmap.md).

### About us

Founded by lovers of innovation and design, [Agence Dn'D] (https://www.dnd.fr) assists companies for 11 years in the creation and development of customized digital (open source) solutions for web and E-commerce.
