<?php


namespace Pimgento\Api\Job;


use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Event\ManagerInterface;
use Magento\Eav\Model\Config;
use Magento\Framework\App\Cache\TypeListInterface;
use Magento\Catalog\Model\Product as ProductModel;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Import\Product as EntitiesHelper;
use Pimgento\Api\Helper\Output as OutputHelper;
use Pimgento\Api\Helper\ProductFilters;
use Pimgento\Api\Helper\Serializer as JsonSerializer;
use Pimgento\Api\Helper\Store as StoreHelper;
use Psr\Log\LoggerInterface;
use Magento\Framework\App\Config\ScopeConfigInterface;
use Zend_Db_Expr as Expr;
use Magento\Catalog\Model\Product\Attribute\Backend\Media\ImageEntryConverter;
use Magento\Catalog\Model\Product\Visibility;

class Image extends Import
{

    /**
     * @var string PIM_PRODUCT_STATUS_DISABLED
     */
    const PIM_PRODUCT_STATUS_DISABLED = '0';
    /**
     * @var string MAGENTO_PRODUCT_STATUS_DISABLED
     */
    const MAGENTO_PRODUCT_STATUS_DISABLED = '2';

    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'image';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Image';
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
     * This variable contains a ProductFilters
     *
     * @var ProductFilters $productFilters
     */
    protected $productFilters;
    /**
     * @var TypeListInterface
     */
    private $cacheTypeList;
    /**
     * @var string $configurableTmpTableSuffix
     */
    protected $configurableTmpTableSuffix;
    /**
     * @var Option
     */
    private $optionJob;
    /**
     * @var ScopeConfigInterface
     */
    private $scopeConfig;
    /**
     * @var JsonSerializer
     */
    private $serializer;
    /**
     * @var StoreHelper
     */
    private $storeHelper;
    /**
     * @var ProductModel
     */
    private $product;

    /**
     * ProductModel constructor
     *
     * @param OutputHelper $outputHelper
     * @param ManagerInterface $eventManager
     * @param Authenticator $authenticator
     * @param LoggerInterface $logger
     * @param EntitiesHelper $entitiesHelper
     * @param ConfigHelper $configHelper
     * @param ProductFilters $productFilters
     * @param TypeListInterface $cacheTypeList
     * @param ScopeConfigInterface $scopeConfig
     * @param JsonSerializer $serializer
     * @param Option $optionJob
     * @param StoreHelper $storeHelper
     * @param ProductModel $product
     * @param array $data
     */
    public function __construct(
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        LoggerInterface $logger,
        EntitiesHelper $entitiesHelper,
        ConfigHelper $configHelper,
        ProductFilters $productFilters,
        TypeListInterface $cacheTypeList,
        ScopeConfigInterface $scopeConfig,
        JsonSerializer $serializer,
        Option $optionJob,
        StoreHelper $storeHelper,
        ProductModel $product,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->entitiesHelper  = $entitiesHelper;
        $this->configHelper    = $configHelper;
        $this->productFilters = $productFilters;
        $this->cacheTypeList = $cacheTypeList;
        $this->configurableTmpTableSuffix = 'image_low_level_configurable';
        $this->optionJob = $optionJob;
        $this->scopeConfig = $scopeConfig;
        $this->serializer = $serializer;
        $this->storeHelper = $storeHelper;
        $this->product = $product;
    }

    /**
    * Create temporary table
    *
    * @return void
    */
    public function createTable()
    {
        /** @var PageInterface $product */
        $product = $this->akeneoClient->getProductApi()->listPerPage(1);
        /** @var array $product */
        $product = $product->getItems();
        if (empty($product)) {
            $this->setMessage(__('No results from Akeneo'));
            $this->stop(1);

            return;
        }
        $product = reset($product);
        $this->entitiesHelper->createTmpTableFromApi($product, $this->getCode());

        $this->entitiesHelper->createTmpTableFromApi($product, $this->configurableTmpTableSuffix);
    }

    /**
     * Insert data into Product temporary table, tmp_pimgento_entities_product
     *
     * @return void
     */
    public function insertProductData()
    {
        /** @var array $filters */
        $filters = $this->productFilters->getFilters();
        /** @var string|int $paginationSize */
        $paginationSize = $this->configHelper->getPanigationSize();
        /** @var ResourceCursorInterface $productModels */
        $products = $this->akeneoClient->getProductApi()->all($paginationSize, $filters);
        /** @var int $index */
        $index = 0;
        /**
         * @var int $index
         * @var array $product
         */
        foreach ($products as $index => $product) {
            $this->entitiesHelper->insertDataFromApi($product, $this->getCode());
        }
        if ($index) {
            $index++;
        }

        $this->setMessage(
            __('%1 line(s) found', $index)
        );
    }

    /**
     * Import options for the (metric) attributes that were forked
     * during the FamilyVariant import job.
     *
     * @throws \Zend_Db_Statement_Exception
     * @return void
     */
    public function importForkedAttributeOptions()
    {
        /** @var \Magento\Framework\DB\Adapter\AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $productTmpTable */
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());

        // What attributes have been forked during the FamilyVariant import job?
        // $attributeForks contains K=>V pairs like so:
        // ['code_fork'=>FORKED_CODE, 'code_orig'=> ORIGINAL_CODE, 'unit'=>UNIT]

        /** @var Select $select */
        $select = $connection->select()->from(
            FamilyVariant::FORKED_ATTRIBUTE_TABLE_NAME,
            [
                'code_fork' => 'code' . FamilyVariant::FAMILY_FORK_SUFFIX,
                'code_orig' => 'code',
            ]
        )   ->where('code' . FamilyVariant::FAMILY_FORK_SUFFIX . ' IS NOT NULL')
            ->where('code' . FamilyVariant::FAMILY_FORK_SUFFIX . ' <> ""');
        /** @var array $data */
        $attributeForks = $connection->fetchAssoc($select);

        if (count($attributeForks)) {

            // Keep only those forked attributes that are present in the Product tmp table.

            foreach ($attributeForks as $forkedCode => $attributeFork) {
                if (!$connection->tableColumnExists($productTmpTable, $attributeFork['code_orig'])) {
                    unset($attributeForks[$forkedCode]);
                }
            }

            if (count($attributeForks)) {

                // Prepare to write the new options to an Options tmp table.
                $this->optionJob->createTable();

                // Get the locales for which the options will need labels.
                /** @var array $optionsTableColumns */
                $optionsTableColumns = array_keys(
                    $connection->describeTable(
                        $this->entitiesHelper->getTableName($this->optionJob->getCode())
                    // Describing table tmp_pimgento_entities_option
                    )
                );
                /** @var array $localeSuffixes */
                $localeSuffixes = [];
                foreach ($optionsTableColumns as $title) {
                    $parts = explode('-', $title);
                    if ($parts[0] === 'labels') {
                        $localeSuffixes[] = $parts[1];
                    }
                }

                // Fetch all units from the API.

                $measureFamilies = $this->akeneoClient->getMeasureFamilyApi()->all();

                foreach ($attributeForks as $forkedCode => $attributeFork) {

                    // On the Product tmp table front, fork necessary columns.

                    $this->entitiesHelper->copyColumn($productTmpTable, $attributeFork['code_orig'], $forkedCode);

                    // Make a list of all options present in the Product tmp table.

                    $select = $connection->select()
                        ->distinct()
                        ->from($productTmpTable, $forkedCode)
                        ->where($forkedCode . '!=""')
                        ->where($forkedCode . ' IS NOT NULL');
                    /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
                    $query = $connection->query($select);

                    while ($row = $query->fetch()) {
                        $option = $row[$forkedCode];

                        // Separate unit from amount
                        $matches = [];
                        preg_match('#(.*) ([^ ]+)$#U', $option, $matches);
                        list(, $amount, $unitCode) = $matches;

                        // Query API for unit symbol matching the code.
                        $unitSymbol = '';
                        foreach ($measureFamilies as $measureFamily) {
                            foreach ($measureFamily['units'] as $apiUnit) {
                                if ($unitCode === $apiUnit['code']) {
                                    $unitSymbol = $apiUnit['symbol'];
                                    break 2;
                                }
                            }
                        }

                        $data = [
                            'code'          => $option,
                            'attribute'     => $forkedCode,
                        ];
                        // Add labels for each locale.
                        foreach ($localeSuffixes as $localeSuffix) {
                            $data['labels-' . $localeSuffix] = $amount . ' ' . $unitSymbol;
                        }
                        // Write data to the Options tmp table.
                        $connection->insertOnDuplicate(
                            $this->entitiesHelper->getTableName($this->optionJob->getCode()),
                            $data
                        );

                    }

                }

                // Complete the Options import job.
                $this->optionJob->runFromStep(3);
            }
        }
    }

    /**
     * Enrich temporary tables before processing
     *
     * @return void
     * @throws LocalizedException
     */
    public function addRequiredData()
    {
        /** @var \Magento\Framework\DB\Adapter\AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $productTmpTable */
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $configurableTmpTable */
        $configurableTmpTable = $this->entitiesHelper->getTableName($this->configurableTmpTableSuffix);
        /** @var array $tmpTables */
        $tmpTables = [$productTmpTable, $configurableTmpTable];

        foreach ($tmpTables as $tmpTable) {
            $connection->addColumn($tmpTable, '_type_id', [
                // This column holds info as to whether a product is "simple" or "configurable"
                'type' => 'text',
                'length' => 255,
                'default' => 'simple',
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_options_container', [
                'type' => 'text',
                'length' => 255,
                'default' => 'container2',
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_tax_class_id', [
                'type' => 'integer',
                'length' => 11,
                'default' => 0,
                'COMMENT' => ' ',
                'nullable' => false
            ]);// None
            $connection->addColumn($tmpTable, '_attribute_set_id', [
                'type' => 'integer',
                'length' => 11,
                'default' => 4,
                'COMMENT' => ' ',
                'nullable' => false
            ]);// Default
            $connection->addColumn($tmpTable, '_visibility', [
                'type' => 'integer',
                'length' => 11,
                'default' => Visibility::VISIBILITY_BOTH,
                'COMMENT' => ' ',
                'nullable' => false
            ]);
            $connection->addColumn($tmpTable, '_status', [
                'type' => 'integer',
                'length' => 11,
                'default' => 2,
                'COMMENT' => ' ',
                'nullable' => false
            ]); // Disabled
            if (!$connection->tableColumnExists($tmpTable, 'url_key')) {
                $connection->addColumn($tmpTable, 'url_key', [
                    'type' => 'text',
                    'length' => 255,
                    'default' => '',
                    'COMMENT' => ' ',
                    'nullable' => false
                ]);
                $connection->update($tmpTable, ['url_key' => new Expr('LOWER(`identifier`)')]);
            }
            if ($connection->tableColumnExists($tmpTable, 'enabled')) {
                $connection->update($tmpTable, ['_status' => new Expr('IF(`enabled` <> 1, 2, 1)')]);
            }

            // The _children column will hold a list referencing all children of the configurable.
            $connection->addColumn($tmpTable, '_children', 'text');
            // The _axis column will hold a list of variation axes applicable to the configurable's children
            $connection->addColumn($tmpTable, '_axis', 'text');

            /** @var string|null $groupColumn */
            $groupColumn = null;

            if ($connection->tableColumnExists($tmpTable, 'type_id')) {
                /** @var string $types */
                $types = $connection->quote($this->allowedTypeId); // Possible values: "simple", "virtual".
                $connection->update(
                    $tmpTable,
                    [
                        // Set "simple" product type.
                        '_type_id' => new Expr("IF(`type_id` IN ($types), `type_id`, 'simple')"),
                    ]
                );
            }


        }
    }

    /**
     * Map PIM attributes to Magento attributes for simple products,
     * i.e. copy columns of tmp_pimgento_entities_product, named after PIM attributes,
     * to new columns named after magento attributes, as defined in the Pimgento-settings GUI.
     */
    public function mapAttributesForSimpleProducts()
    {
        /** @var string $productTmpTable */
        $productTmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $configurableTmpTable */
        $configurableTmpTable = $this->entitiesHelper->getTableName($this->configurableTmpTableSuffix);
        /** @var array $tmpTables */
        $tmpTables = [$productTmpTable, $configurableTmpTable];

        /** @var string|array $matches */
        $matches = $this->scopeConfig->getValue(ConfigHelper::PRODUCT_ATTRIBUTE_MAPPING_SIMPLE);
        $matches = $this->serializer->unserialize($matches);

        // $matches is an array like [['pim_attribute' => value, 'magento_attribute' => value], ..]
        if (!is_array($matches)) {
            return;
        }
        /** @var array $match */
        foreach ($matches as $match) {
            if (!isset($match['pim_attribute'], $match['magento_attribute'])) {
                continue;
            }
            /** @var string $pimAttribute */
            $pimAttribute = $match['pim_attribute'];
            /** @var string $magentoAttribute */
            $magentoAttribute = $match['magento_attribute'];
            foreach ($tmpTables as $tmpTable) {
                $this->entitiesHelper->copyColumn($tmpTable, $pimAttribute, $magentoAttribute);
            }
        }
    }

    /**
     * Match code with entity
     *
     * @return void
     */
    public function matchEntities()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        /** @var array $duplicates */
        $duplicates = $connection->fetchCol(
            $connection->select()
                ->from($tmpTable, ['identifier'])
                ->group('identifier')
                ->having('COUNT(identifier) > ?', 1)
        );

        if (!empty($duplicates)) {
            $this->setMessage(
                __('Duplicates sku detected. Make sure Product Model code is not used for a simple product sku. Duplicates: %1', join(', ', $duplicates))
            );
            $this->stop(true);

            return;
        }


        $this->entitiesHelper->matchEntitiesForImage(
            'identifier',
            'catalog_product_entity',
            'entity_id',
            $this->getCode()
        );
    }

    public function importImages()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var string $tableName */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());

        /** @var string $table */
        $table = $this->entitiesHelper->getTable('catalog_product_entity');
        /** @var string $columnIdentifier */
        $columnIdentifier = $this->entitiesHelper->getColumnIdentifier($table);

        /** @var array $data */
        $data = [
            $columnIdentifier => '_entity_id',
            'sku'             => 'identifier',
        ];
        if (!$connection->tableColumnExists($tmpTable, 'icecat_images')) {
            $this->setMessage(__('Warning: icecat_images attribute does not exist'));
        }
        $data['icecat_images'] = 'icecat_images';

        /** @var \Magento\Framework\DB\Select $select */
        $select = $connection->select()->from($tmpTable, $data);

        /** @var \Magento\Framework\DB\Statement\Pdo\Mysql $query */
        $query = $connection->query($select);

        /** @var \Magento\Catalog\Model\ResourceModel\Eav\Attribute $galleryAttribute */
        $galleryAttribute = $this->configHelper->getAttribute(ProductModel::ENTITY, 'media_gallery');
        /** @var string $galleryTable */
        $galleryTable = $this->entitiesHelper->getTable('catalog_product_entity_media_gallery');
        /** @var string $galleryEntityTable */
        $galleryEntityTable = $this->entitiesHelper->getTable('catalog_product_entity_media_gallery_value_to_entity');
        /** @var string $galleryValueTable */
        $galleryValueTable = $this->entitiesHelper->getTable('catalog_product_entity_media_gallery_value');
        /** @var string $productImageTable */
        $productImageTable = $this->entitiesHelper->getTable('catalog_product_entity_varchar');

        /** @var array $row */
        while (($row = $query->fetch())) {
            /** @var array $files */
            $files = [];

            if (!isset($row['icecat_images'])) {
                continue;
            }

            if (!$row['icecat_images']) {
                continue;
            }

            /** @var array $assets */
            $assets = explode(',', $row['icecat_images']);
            $key = 0;
            foreach ($assets as $asset) {
                $name = basename($asset);
                if (!$this->configHelper->mediaFileExists($name)) {
                    $binary = file_get_contents($asset);
                    $this->configHelper->saveMediaFile($name, $binary);
                }

                /** @var string $file */
                $file = $this->configHelper->getMediaFilePath($name);

                /** @var int $valueId */
                $valueId = $connection->fetchOne(
                    $connection->select()
                        ->from($galleryTable, ['value_id'])
                        ->where('value = ?', $file)
                );

                if (!$valueId) {
                    /** @var int $valueId */
                    $valueId = $connection->fetchOne(
                        $connection->select()->from($galleryTable, [new Expr('MAX(`value_id`)')])
                    );
                    $valueId += 1;
                }

                /** @var array $data */
                $data = [
                    'value_id' => $valueId,
                    'attribute_id' => $galleryAttribute->getId(),
                    'value' => $file,
                    'media_type' => ImageEntryConverter::MEDIA_TYPE_CODE,
                    'disabled' => 0,
                ];
                $connection->insertOnDuplicate($galleryTable, $data, array_keys($data));

                /** @var array $data */
                $data = [
                    'value_id'        => $valueId,
                    $columnIdentifier => $row[$columnIdentifier]
                ];

                $connection->insertOnDuplicate($galleryEntityTable, $data, array_keys($data));
                $storeArray = $this->storeHelper->getStores();
                foreach ($storeArray as $stores){
                    foreach ($stores as $store){

                        /** @var array $data */
                        $data = [
                            'value_id'        => $valueId,
                            'store_id'        => $store['store_id'],
                            $columnIdentifier => $row[$columnIdentifier],
                            'label'           => Null,
                            'position'        => $key,
                            'disabled'        => 0,
                        ];
                        $connection->insertOnDuplicate($galleryValueTable, $data, array_keys($data));

                        $key++;
                        if (empty($files)) {
                            /** @var array $entities */
                            $attributes = [
                                $this->configHelper->getAttribute(ProductModel::ENTITY, 'image'),
                                $this->configHelper->getAttribute(ProductModel::ENTITY, 'small_image'),
                                $this->configHelper->getAttribute(ProductModel::ENTITY, 'thumbnail'),
                            ];

                            foreach ($attributes as $attribute) {
                                if (!$attribute) {
                                    continue;
                                }
                                /** @var array $data */
                                $data = [
                                    'attribute_id'    => $attribute->getId(),
                                    'store_id'        => $store['store_id'],
                                    $columnIdentifier => $row[$columnIdentifier],
                                    'value'           => $file
                                ];
                                $connection->insertOnDuplicate($productImageTable, $data, array_keys($data));
                            }
                        }
                    }
                }
                $files[] = $file;
            }
        }

        /** @var \Magento\Framework\DB\Select $cleaner */
        $cleaner = $connection->select()
            ->from($galleryTable, ['value_id'])
            ->where('value NOT IN (?)', $files);

        $connection->delete(
            $galleryEntityTable,
            [
                'value_id IN (?)'          => $cleaner,
                $columnIdentifier . ' = ?' => $row[$columnIdentifier]
            ]
        );
    }

    /**
     * Drop temporary table
     *
     * @return void
     */
    public function dropTable()
    {
        $this->entitiesHelper->dropTable($this->getCode());
    }

}
