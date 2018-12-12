<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Eav\Model\Entity\Attribute\ScopedAttributeInterface;
use Magento\Eav\Setup\EavSetup;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\Framework\DB\Select;
use Magento\Framework\Event\ManagerInterface;
use Magento\Eav\Model\Config;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Import\Attribute as AttributeHelper;
use Pimgento\Api\Helper\Import\Entities as EntitiesHelper;
use Pimgento\Api\Helper\Output as OutputHelper;
use Pimgento\Api\Helper\Store as StoreHelper;
use \Zend_Db_Expr as Expr;

/**
 * Class Attribute
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class Attribute extends Import
{
    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'attribute';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Attribute';
    /**
     * This variable contains an EntitiesHelper
     *
     * @var EntitiesHelper $entitiesHelper
     */
    protected $entitiesHelper;
    /**
     * This variable contains a ConfigHelper
     *
     * @var ConfigHelper $configHelper
     */
    protected $configHelper;
    /**
     * This variable contains a Config
     *
     * @var Config $eavConfig
     */
    protected $eavConfig;
    /**
     * This variable contains an AttributeHelper
     *
     * @var AttributeHelper $attributeHelper
     */
    protected $attributeHelper;
    /**
     * This variable contains a TypeListInterface
     *
     * @var TypeListInterface $cacheTypeList
     */
    protected $cacheTypeList;
    /**
     * This variable contains a StoreHelper
     *
     * @var StoreHelper $storeHelper
     */
    protected $storeHelper;
    /**
     * This variable contains an EavSetup
     *
     * @var EavSetup $eavSetup
     */
    protected $eavSetup;

    /**
     * @var \Psr\Log\LoggerInterface $logger
     */
    protected $logger;

    /**
     * Attribute constructor
     *
     * @param OutputHelper $outputHelper
     * @param ManagerInterface $eventManager
     * @param Authenticator $authenticator
     * @param EntitiesHelper $entitiesHelper
     * @param ConfigHelper $configHelper
     * @param Config $eavConfig
     * @param AttributeHelper $attributeHelper
     * @param TypeListInterface $cacheTypeList
     * @param StoreHelper $storeHelper
     * @param EavSetup $eavSetup
     * @param \Psr\Log\LoggerInterface $logger
     * @param array $data
     */
    public function __construct(
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        EntitiesHelper $entitiesHelper,
        ConfigHelper $configHelper,
        Config $eavConfig,
        AttributeHelper $attributeHelper,
        TypeListInterface $cacheTypeList,
        StoreHelper $storeHelper,
        EavSetup $eavSetup,
        \Psr\Log\LoggerInterface $logger,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $data);

        $this->entitiesHelper  = $entitiesHelper;
        $this->configHelper    = $configHelper;
        $this->eavConfig       = $eavConfig;
        $this->attributeHelper = $attributeHelper;
        $this->cacheTypeList   = $cacheTypeList;
        $this->storeHelper     = $storeHelper;
        $this->eavSetup        = $eavSetup;
        $this->logger          = $logger;
    }

    /**
     * Create temporary table
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $attributes */
        $attributes = $this->akeneoClient->getAttributeApi()->listPerPage(1);
        /** @var array $attribute */
        $attribute = $attributes->getItems();
        if (empty($attribute)) {
            $this->setMessage(__('No results from Akeneo'));
            $this->stop(1);

            return;
        }
        $attribute = reset($attribute);
        $this->entitiesHelper->createTmpTableFromApi($attribute, $this->getCode());
    }

    /**
     * Insert data into temporary table
     *
     * @return void
     */
    public function insertData()
    {
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var ResourceCursorInterface $attributes */
        $attributes = $this->akeneoClient->getAttributeApi()->all($paginationSize);

        /**
         * @var int $index
         * @var array $attribute
         */
        foreach ($attributes as $index => $attribute) {
            $this->entitiesHelper->insertDataFromApi($attribute, $this->getCode());
        }
        $index++;

        $this->setMessage(
            __('%1 line(s) found', $index)
        );
    }

    /**
     * Match code with entity
     * The pimgento_entities table keeps track of PIM objects that are already mapped to magento entities,
     * based on their code (e.g. "capacity").
     * It makes sure new rows found in tmp_pimgento_entities_attribute are identified as such and assigned
     * an _entity_id (= pimgento_entities.entity_id) that is consistent with the sequence of the table into which they must ultimately be written
     * (eav_attribute).
     *
     *
     * @return void
     */
    public function matchEntities()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var Select $select */
        $select = $connection->select()->from(
            $connection->getTableName('eav_attribute'),
            [
                'import'    => new Expr('"attribute"'),
                'code'      => 'attribute_code',
                'entity_id' => 'attribute_id',
            ]
        )->where('entity_type_id = ?', $this->getEntityTypeId());
        //SELECT
        //    "attribute" AS import,
        //    eav_attribute.attribute_code AS code,
        //    eav_attribute.attribute_id AS entity_id
        //FROM eav_attribute
        //    WHERE (entity_type_id = '4');

        // example code: capacity
        // entity_type_id=4 restricts results to attributes applicable to catalog products.

        $connection->query(
            $connection->insertFromSelect(
                $select,
                $connection->getTableName('pimgento_entities'),
                ['import', 'code', 'entity_id'],
                2
            )
        );

        $this->entitiesHelper->matchEntity('code', 'eav_attribute', 'attribute_id', $this->getCode());
    }

    /**
     * Match type with Magento logic
     * Map each PIM type to an array of values on the Magento side:
     * backend_type, frontend_input, backend_model, source_model, frontend_model
     *
     * @return void
     */
    public function matchType()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $columns */
        $columns = $this->attributeHelper->getSpecificColumns();
        /**
         * @var string $name
         * @var array $def
         */
        foreach ($columns as $name => $def) {
            $connection->addColumn($tmpTable, $name, $def['type']);
        }

        /** @var Select $select */
        $select = $connection->select()->from(
            $tmpTable,
            array_merge(
                ['_entity_id', 'type'],
                array_keys($columns)
            )
        );
        //SELECT
        //    tmp_pimgento_entities_attribute._entity_id,
        //    tmp_pimgento_entities_attribute.type,
        //    tmp_pimgento_entities_attribute.backend_type,
        //    tmp_pimgento_entities_attribute.frontend_input,
        //    tmp_pimgento_entities_attribute.backend_model,
        //    tmp_pimgento_entities_attribute.source_model,
        //    tmp_pimgento_entities_attribute.frontend_model
        //FROM tmp_pimgento_entities_attribute;
        /** @var array $data */
        $data = $connection->fetchAssoc($select);
        /**
         * @var int $id
         * @var array $attribute
         */
        // for each row of tmp_pimgento_entities_attribute,
        // $id contains the value of _entity_id,
        // $attribute contains the whole row.
        foreach ($data as $id => $attribute) {

            // Look up how PIM types are to be mapped to magento types.
            // This informs the following columns of tmp_magento_entities_attribute:
            // backend_type, frontend_input, backend_model, source_model, frontend_model

            /** @var array $type */
            $type = $this->attributeHelper->getType($attribute['type']);
            $this->logger->info('==== matched type ====');
            $this->logger->info(print_r($type, true));
            $connection->update($tmpTable, $type, ['_entity_id = ?' => $id]);
        }
    }

    /**
     * Match family code with Magento group id
     * Ensure that, on the Magento side, attributes are connected to the relevant family, e.g. "phone", "computer"...
     * Some attributes might be relevant to certain families but not to others.
     *
     * @return void
     */
    public function matchFamily()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $familyAttributeRelationsTable */
        $familyAttributeRelationsTable = $connection->getTableName('pimgento_family_attribute_relations');

        $connection->addColumn($tmpTable, '_attribute_set_id', 'text');
        /** @var string $importTmpTable */
        $importTmpTable = $connection->select()->from($tmpTable, ['code', '_entity_id']);
        // Example row: code=>capacity; _entity_id=>141
        /** @var string $queryTmpTable */
        $queryTmpTable = $connection->query($importTmpTable);

        while ($row = $queryTmpTable->fetch()) {
            /** @var string $attributeCode */
            $attributeCode = $row['code'];
            /** @var Select $importRelations */
            $importRelations = $connection->select()->from($familyAttributeRelationsTable, 'family_entity_id')->where(
                $connection->prepareSqlCondition('attribute_code', ['like' => $attributeCode])
            );
            /** @var \Zend_Db_Statement_Interface $queryRelations */
            $queryRelations = $connection->query($importRelations);
            /** @var string $attributeIds */
            $attributeIds = '';
            while ($innerRow = $queryRelations->fetch()) {
                $attributeIds .= $innerRow['family_entity_id'] . ',';
            }
            $attributeIds = rtrim($attributeIds, ',');

            // tmp_pimgento_entities_attribute._attribute_set_id contains a list of families ("phone, "computer", etc.)
            // to which the attribute is relevant.
            $connection->update($tmpTable, ['_attribute_set_id' => $attributeIds], '_entity_id=' . $row['_entity_id']);
        }
    }

    /**
     * Add attributes if not exists
     *
     * @return void
     */
    public function addAttributes()
    {
        // Preparing to add columns not present in the tmp table but necessary in eav_attribute
        /** @var array $columns */
        $columns = $this->attributeHelper->getSpecificColumns();
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var Select $import */
        $import = $connection->select()->from($tmpTable);
        /** @var \Zend_Db_Statement_Interface $query */
        $query = $connection->query($import);

        while (($row = $query->fetch())) {
            /* Insert base data (ignore if already exists) */

            // Attributes are inserted into 2 distinct tables:
            // eav_attribute, catalog_eav_attribute
            //
            // catalog_eav_attribute is an extension of the eav_attribute table.
            // The eav_attribute table contains only the general data for all the attributes of all the EAV entities
            // (category, product, customer, customer address, anything custom).
            // Each attribute's properties are determined by the fields in the eav_attribute table
            // plus the fields in catalog_eav_attribute.
            //
            // Additionnally, attributes may have to be referenced in
            // eav_attribute_group and eav_entity_attribute

            /** @var string[] $values */
            $values = [
                'attribute_id'   => $row['_entity_id'], // = pimgento_entities.entity_id
                'entity_type_id' => $this->getEntityTypeId(), // value: "4", for catalog_product
                'attribute_code' => $row['code'],
            ];

            // Insert/update basic data in primary EAV attribute table.
            $connection->insertOnDuplicate(
                $connection->getTableName('eav_attribute'),
                $values,
                array_keys($values)
            );

            // Insert/update PK in primary EAV attribute table.
            $values = [
                'attribute_id' => $row['_entity_id'],
            ];
            $connection->insertOnDuplicate(
                $connection->getTableName('catalog_eav_attribute'),
                $values,
                array_keys($values)
            );

            // Prepare additional data for both tables.

            /* Retrieve default admin label */
            /** @var array $stores */
            $stores = $this->storeHelper->getStores();
            /** @var string $frontendLabel */
            $frontendLabel = __('Unknown');
            if (isset($stores[0])) {
                /** @var array $admin */
                $admin = reset($stores[0]);
                if (isset($row['labels-'.$admin['lang']])) {
                    $frontendLabel = $row['labels-'.$admin['lang']];
                }
            }

            /* Retrieve attribute scope */
            /** @var int $global */
            $global = ScopedAttributeInterface::SCOPE_GLOBAL; // Global
            if ($row['scopable'] == 1) {
                $global = ScopedAttributeInterface::SCOPE_WEBSITE; // Website
            }
            if ($row['localizable'] == 1) {
                $global = ScopedAttributeInterface::SCOPE_STORE; // Store View
            }
            /** @var array $data */
            $data = [
                'entity_type_id' => $this->getEntityTypeId(),
                'attribute_code' => $row['code'],
                'frontend_label' => $frontendLabel,
                'is_global'      => $global,
            ];
            foreach ($columns as $column => $def) {
                if (!$def['only_init']) {
                    $data[$column] = $row[$column];
                }
            }
            /** @var array $defaultValues */
            $defaultValues = [];
            if ($row['_is_new'] == 1) {
                $defaultValues = [
                    // For insert into eav_attribute.
                    'backend_table'                 => null,
                    'frontend_class'                => null,
                    'is_required'                   => 0,
                    'is_user_defined'               => 1,
                    'default_value'                 => null,
                    'is_unique'                     => $row['unique'],
                    'note'                          => null,
                    // For insert into catalog_eav_attribute.
                    'is_visible'                    => 1,
                    'is_system'                     => 1,
                    'input_filter'                  => null,
                    'multiline_count'               => 0,
                    'validate_rules'                => null,
                    'data_model'                    => null,
                    'sort_order'                    => 0,
                    'is_used_in_grid'               => 0,
                    'is_visible_in_grid'            => 0,
                    'is_filterable_in_grid'         => 0,
                    'is_searchable_in_grid'         => 0,
                    'frontend_input_renderer'       => null,
                    'is_searchable'                 => 0,
                    'is_filterable'                 => 0,
                    'is_comparable'                 => 0,
                    'is_visible_on_front'           => 0,
                    'is_wysiwyg_enabled'            => 0,
                    'is_html_allowed_on_front'      => 0,
                    'is_visible_in_advanced_search' => 0,
                    'is_filterable_in_search'       => 0,
                    'used_in_product_listing'       => 0,
                    'used_for_sort_by'              => 0,
                    'apply_to'                      => null,
                    'position'                      => 0,
                    'is_used_for_promo_rules'       => 0,
                ];

                foreach (array_keys($columns) as $column) {
                    $data[$column] = $row[$column];
                    // $data : for insertion into eav_attribute and catalog_eav_attribute.
                    // $row: read from tmp_pimgento_entities_attribute.
                }
            }

            // Set default values for data keys that are not columns of tmp_pimgento_entities_attribute.
            $data = array_merge($defaultValues, $data);

            // Update both tables at once.

            $this->eavSetup->updateAttribute(
                $this->getEntityTypeId(),
                $row['_entity_id'],
                $data,
                null,
                0
            );

            /* Add Attribute to group and family */
            // Not applicable to us, since we don't seem to have an _attribute_set_id column in tmp_pimgento_entities_attribute.
            if ($row['_attribute_set_id'] && $row['group']) {
                $attributeSetIds = explode(',', $row['_attribute_set_id']);

                if (is_numeric($row['group'])) {
                    $row['group'] = 'PIM' . $row['group'];
                }

                foreach ($attributeSetIds as $attributeSetId) {
                    if (is_numeric($attributeSetId)) {
                        $this->eavSetup->addAttributeGroup(
                            $this->getEntityTypeId(),
                            $attributeSetId,
                            ucfirst($row['group'])
                        );
                        $this->eavSetup->addAttributeToSet(
                            $this->getEntityTypeId(),
                            $attributeSetId,
                            ucfirst($row['group']),
                            $row['_entity_id']
                        );
                    }
                }
            }

            /* Add store labels */
            /** @var array $stores */
            $stores = $this->storeHelper->getStores('lang');
            /**
             * @var string $lang
             * @var array $data
             */
            foreach ($stores as $lang => $data) {
                if (isset($row['labels-'.$lang])) {
                    /** @var array $store */
                    foreach ($data as $store) {
                        /** @var string $exists */
                        $exists = $connection->fetchOne(
                            $connection->select()->from($connection->getTableName('eav_attribute_label'))->where(
                                'attribute_id = ?',
                                $row['_entity_id']
                            )->where('store_id = ?', $store['store_id'])
                        );

                        if ($exists) {
                            /** @var array $values */
                            $values = [
                                'value' => $row['labels-'.$lang],
                            ];
                            /** @var array $where */
                            $where  = [
                                'attribute_id = ?' => $row['_entity_id'],
                                'store_id = ?'     => $store['store_id'],
                            ];

                            $connection->update($connection->getTableName('eav_attribute_label'), $values, $where);
                        } else {
                            $values = [
                                'attribute_id' => $row['_entity_id'],
                                'store_id'     => $store['store_id'],
                                'value'        => $row['labels-'.$lang],
                            ];
                            $connection->insert($connection->getTableName('eav_attribute_label'), $values);
                        }
                    }
                }
            }
        }
    }

    /**
     * Drop temporary table
     *
     * @return void
     */
    public function dropTable()
    {
//        $this->entitiesHelper->dropTable($this->getCode());
    }

    /**
     * Clean cache
     *
     * @return void
     */
    public function cleanCache()
    {
        /** @var string[] $types */
        $types = [
            \Magento\Framework\App\Cache\Type\Block::TYPE_IDENTIFIER,
            \Magento\PageCache\Model\Cache\Type::TYPE_IDENTIFIER,
        ];
        /** @var string $type */
        foreach ($types as $type) {
            $this->cacheTypeList->cleanType($type);
        }

        $this->setMessage(
            __('Cache cleaned for: %1', join(', ', $types))
        );
    }

    /**
     * Get the product entity type id
     *
     * @return string
     */
    protected function getEntityTypeId()
    {
        /** @var string $productEntityTypeId */
        $productEntityTypeId = $this->eavConfig->getEntityType(ProductAttributeInterface::ENTITY_TYPE_CODE)
            ->getEntityTypeId();

        return $productEntityTypeId;
    }
}
