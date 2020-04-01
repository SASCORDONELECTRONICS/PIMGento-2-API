<?php

namespace Pimgento\Api\Job;

use Akeneo\Pim\ApiClient\Pagination\PageInterface;
use Akeneo\Pim\ApiClient\Pagination\ResourceCursorInterface;
use Magento\Catalog\Api\Data\ProductAttributeInterface;
use Magento\Framework\DB\Adapter\AdapterInterface;
use Magento\Framework\Event\ManagerInterface;
use Magento\Eav\Model\Config;
use Pimgento\Api\Helper\Authenticator;
use Pimgento\Api\Helper\Config as ConfigHelper;
use Pimgento\Api\Helper\Import\Entities as EntitiesHelper;
use Pimgento\Api\Helper\Output as OutputHelper;

/**
 * Class ProductModel
 *
 * @category  Class
 * @package   Pimgento\Api\Job
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class ProductModel extends Import
{
    /**
     * @var int BATCH_SIZE
     */
    const BATCH_SIZE = 500;

    /**
     * This variable contains a string value
     *
     * @var string $code
     */
    protected $code = 'product_model';
    /**
     * This variable contains a string value
     *
     * @var string $name
     */
    protected $name = 'Product Model';
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
     * ProductModel constructor
     *
     * @param OutputHelper                        $outputHelper
     * @param ManagerInterface                    $eventManager
     * @param Authenticator                       $authenticator
     * @param \Psr\Log\LoggerInterface            $logger
     * @param \Pimgento\Api\Helper\Import\Product $entitiesHelper
     * @param ConfigHelper                        $configHelper
     * @param Config                              $eavConfig
     * @param array                               $data
     */
    public function __construct(
        OutputHelper $outputHelper,
        ManagerInterface $eventManager,
        Authenticator $authenticator,
        \Psr\Log\LoggerInterface $logger,
        \Pimgento\Api\Helper\Import\Product $entitiesHelper,
        ConfigHelper $configHelper,
        Config $eavConfig,
        array $data = []
    ) {
        parent::__construct($outputHelper, $eventManager, $authenticator, $logger, $data);

        $this->entitiesHelper  = $entitiesHelper;
        $this->configHelper    = $configHelper;
        $this->eavConfig       = $eavConfig;
    }

    /**
     * Create temporary table
     *
     * @return void
     */
    public function createTable()
    {
        /** @var PageInterface $productModels */
        $productModels = $this->akeneoClient->getProductModelApi()->listPerPage(1);
        /** @var array $productModel */
        $productModel = $productModels->getItems();
        if (empty($productModel)) {
            $this->setMessage(__('No results from Akeneo'));
            $this->stop(1);

            return;
        }
        $productModel = reset($productModel);
        $this->entitiesHelper->createTmpTableFromApi($productModel, $this->getCode());
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
        /** @var ResourceCursorInterface $productModels */
        $productModels = $this->akeneoClient->getProductModelApi()->all($paginationSize);
        /**
         * @var int   $index
         * @var array $productModel
         */
        foreach ($productModels as $index => $productModel) {
            $this->entitiesHelper->insertDataFromApi($productModel, $this->getCode());
        }
        $index++;
        $this->setMessage(
            __('%1 line(s) found', $index)
        );
    }

    /**
     * Remove columns from product model table
     *
     * @return void
     */
    public function removeColumns()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var array $except */
        $except = ['code', 'axis'];
        /** @var string $variantTable */
        $variantTable = $this->entitiesHelper->getTable('pimgento_product_model');
        /** @var array $columns */
        $columns = array_keys($connection->describeTable($variantTable));
        /** @var string $column */
        foreach ($columns as $column) {
            if (in_array($column, $except)) {
                continue;
            }
            $connection->dropColumn($variantTable, $column);
        }
    }

    /**
     * Add columns to product model table
     *
     * @return void
     */
    public function addColumns()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var array $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var array $except */
        $except = ['code', 'axis', 'type', '_entity_id', '_is_new'];
        /** @var string $variantTable */
        $variantTable = $this->entitiesHelper->getTable('pimgento_product_model');
        /** @var array $columns */
        $columns = array_keys($connection->describeTable($tmpTable));
        /** @var string $column */
        foreach ($columns as $column) {
            if (in_array($column, $except)) {
                continue;
            }
            $connection->addColumn($variantTable, $this->_columnName($column), 'text');
        }
        if (!$connection->tableColumnExists($tmpTable, 'axis')) {
            $connection->addColumn($tmpTable, 'axis', [
                'type' => 'text',
                'length' => 255,
                'default' => '',
                'COMMENT' => ' '
            ]);
        }
    }

    /**
     * Add or update data in product model table
     *
     * @return void
     */
    public function updateData()
    {
        /** @var AdapterInterface $connection */
        $connection = $this->entitiesHelper->getConnection();
        /** @var array $tmpTable */
        $tmpTable = $this->entitiesHelper->getTableName($this->getCode());
        /** @var string $variantTable */
        $variantTable = $this->entitiesHelper->getTable('pimgento_product_model');
        /** @var array $variant */
        $variant = $connection->query(
            $connection->select()->from($tmpTable)
        );
        /** @var array $attributes */
        // $attributes contains key=>value pairs of the type 'color'=>'93'
        // listing variation axes.
        $attributes = $connection->fetchPairs(
            $connection->select()->from(
                $this->entitiesHelper->getTable('eav_attribute'),
                ['attribute_code', 'attribute_id']
            )->where('entity_type_id = ?', $this->getEntityTypeId())
        );
        /** @var array $columns */
        $columns = array_keys($connection->describeTable($tmpTable));
        /** @var array $values */
        // Each element of $values stores the column values of a row of the temporary table tmp_pimgento_entities_product_model,
        // Each element is an array key=>value pairs, of the type 'column name'=>'column value'.
        $values = [];
        /** @var int $i */
        $i = 0;
        /** @var array $keys */
        $keys = [];
        while (($row = $variant->fetch())) {
            $values[$i] = [];
            /** @var int $column */
            foreach ($columns as $column) {
                if ($connection->tableColumnExists($variantTable, $this->_columnName($column))) {
                    if ($column != 'axis') {
                        $values[$i][$this->_columnName($column)] = $row[$column];
                    }
                    if ($column == 'axis' && !$connection->tableColumnExists($tmpTable, 'family_variant')) {
                        /** @var array $axisAttributes */
                        // $axisAttributes might hold an array of codes, such as ['93', '141', '149'].
                        $axisAttributes = explode(',', $row['axis']);
                        /** @var array $axis */
                        $axis = [];
                        /** @var string $code */
                        foreach ($axisAttributes as $code) {
                            if (isset($attributes[$code])) {
                                $axis[] = $attributes[$code];
                            }
                        }
                        // $axis might hold a String listing variation axes such as '93, 141, 149'.
                        // The point of the processing above was to keep only variation axes
                        // that pertain to a catalog_product, filtering out others (e.g. for instance
                        // attributes pertaining to a customer or an order.
                        $values[$i][$column] = join(',', $axis);
                    }
                    $keys = array_keys($values[$i]);
                }
            }
            // Insertion into pimgento_product_model.
            $i++;
            if (count($values) > self::BATCH_SIZE) {
                $connection->insertOnDuplicate($variantTable, $values, $keys);
                $values = [];
                $i      = 0;
            }
        }
        if (count($values) > 0) {
            $connection->insertOnDuplicate($variantTable, $values, $keys);
        }
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

    /**
     * Replace column name
     *
     * @param string $column
     *
     * @return string
     */
    protected function _columnName($column)
    {
        /** @var array $matches */
        $matches = [
            'label' => 'name',
        ];
        /**
         * @var string $name
         * @var string $replace
         */
        foreach ($matches as $name => $replace) {
            if (preg_match('/^' . $name . '/', $column)) {
                /** @var string $column */
                $column = preg_replace('/^' . $name . '/', $replace, $column);
            }
        }

        return $column;
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
