<?php

namespace Pimgento\Api\Helper;

use Magento\Framework\App\Helper\AbstractHelper;
use Magento\Framework\App\Helper\Context;
use Magento\Framework\Encryption\Encryptor;
use Magento\Directory\Helper\Data;
use Magento\Store\Api\Data\StoreInterface;
use Magento\Store\Api\Data\WebsiteInterface;
use Magento\Store\Model\ScopeInterface;
use Magento\Directory\Model\Currency;
use Magento\Eav\Model\Config as EavConfig;
use Magento\Store\Model\StoreManagerInterface;
use Magento\CatalogInventory\Model\Configuration as CatalogInventoryConfiguration;
use Magento\Catalog\Model\Product\Media\Config as MediaConfig;
use Magento\Framework\Filesystem;
use Magento\Framework\Filesystem\Directory\WriteInterface;
use Magento\Framework\File\Uploader;
use Magento\Framework\App\Filesystem\DirectoryList;
use Magento\Eav\Model\Entity\Attribute\AbstractAttribute;
use Magento\Catalog\Helper\Product as ProductHelper;

/**
 * Class Config
 *
 * @category  Class
 * @package   Pimgento\Api\Helper
 * @author    Agence Dn'D <contact@dnd.fr>
 * @copyright 2018 Agence Dn'D
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 * @link      https://www.pimgento.com/
 */
class Config extends AbstractHelper
{
    /** Config keys */
    const AKENEO_API_BASE_URL = 'pimgento/akeneo_api/base_url';
    const AKENEO_API_USERNAME = 'pimgento/akeneo_api/username';
    const AKENEO_API_PASSWORD = 'pimgento/akeneo_api/password';
    const AKENEO_API_CLIENT_ID = 'pimgento/akeneo_api/client_id';
    const AKENEO_API_CLIENT_SECRET = 'pimgento/akeneo_api/client_secret';
    const AKENEO_API_IS_ENTERPRISE = 'pimgento/akeneo_api/is_enterprise';
    const AKENEO_API_PAGINATION_SIZE = 'pimgento/akeneo_api/pagination_size';
    const AKENEO_API_WEBSITE_MAPPING = 'pimgento/akeneo_api/website_mapping';
    const PRODUCTS_FILTERS_MODE = 'pimgento/products_filters/mode';
    const PRODUCTS_FILTERS_COMPLETENESS_TYPE = 'pimgento/products_filters/completeness_type';
    const PRODUCTS_FILTERS_COMPLETENESS_VALUE = 'pimgento/products_filters/completeness_value';
    const PRODUCTS_FILTERS_COMPLETENESS_SCOPE = 'pimgento/products_filters/completeness_scope';
    const PRODUCTS_FILTERS_COMPLETENESS_LOCALES = 'pimgento/products_filters/completeness_locales';
    const PRODUCTS_FILTERS_STATUS = 'pimgento/products_filters/status';
    const PRODUCTS_FILTERS_FAMILIES = 'pimgento/products_filters/families';
    const PRODUCTS_FILTERS_UPDATED = 'pimgento/products_filters/updated';
    const PRODUCTS_FILTERS_ADVANCED_FILTER = 'pimgento/products_filters/advanced_filter';
    const PRODUCT_ATTRIBUTE_MAPPING_SIMPLE = 'pimgento/product/attribute_mapping_simple';
    const PRODUCT_ATTRIBUTE_MAPPING_CONFIGURABLE = 'pimgento/product/attribute_mapping_configurable';
    const ENABLE_CONFIGURABLE_IN_STORE = 'pimgento/product/enable_configurables_in_store';
    const PRODUCT_CONFIGURABLE_ATTRIBUTES = 'pimgento/product/configurable_attributes';
    const PRODUCT_TAX_CLASS = 'pimgento/product/tax_class';
    const PRODUCT_URL_GENERATION_ENABLED = 'pimgento/product/url_generation_enabled';
    const PRODUCT_MEDIA_ENABLED = 'pimgento/product/media_enabled';
    const PRODUCT_MEDIA_IMAGES = 'pimgento/product/media_images';
    const PRODUCT_MEDIA_GALLERY = 'pimgento/product/media_gallery';
    const PRODUCT_ASSET_ENABLED = 'pimgento/product/asset_enabled';
    const PRODUCT_ASSET_GALLERY = 'pimgento/product/asset_gallery';
    const ATTRIBUTE_TYPES = 'pimgento/attribute/types';

    /**
     * @var int PAGINATION_SIZE_DEFAULT_VALUE
     */
    const PAGINATION_SIZE_DEFAULT_VALUE = 10;
    /**
     * This variable contains a Encryptor
     *
     * @var Encryptor $encryptor
     */
    protected $encryptor;
    /**
     * This variable contains a Serializer
     *
     * @var Serializer $serializer
     */
    protected $serializer;
    /**
     * This variable contains a EavConfig
     *
     * @var EavConfig $eavConfig
     */
    protected $eavConfig;
    /**
     * This variable contains a StoreManagerInterface
     *
     * @var StoreManagerInterface $storeManager
     */
    protected $storeManager;
    /**
     * This variable contains a CatalogInventoryConfiguration
     *
     * @var CatalogInventoryConfiguration $catalogInventoryConfiguration
     */
    protected $catalogInventoryConfiguration;
    /**
     * This variable contains a MediaConfig
     *
     * @var MediaConfig $mediaConfig
     */
    protected $mediaConfig;
    /**
     * This variable contains a WriteInterface
     *
     * @var WriteInterface $mediaDirectory
     */
    protected $mediaDirectory;

    /**
     * Config constructor
     *
     * @param Context $context
     * @param Encryptor $encryptor
     * @param Serializer $serializer
     * @param EavConfig $eavConfig
     * @param StoreManagerInterface $storeManager
     * @param CatalogInventoryConfiguration $catalogInventoryConfiguration
     * @param Filesystem $filesystem
     * @param MediaConfig $mediaConfig
     */
    public function __construct(
        Context $context,
        Encryptor $encryptor,
        Serializer $serializer,
        EavConfig $eavConfig,
        StoreManagerInterface $storeManager,
        CatalogInventoryConfiguration $catalogInventoryConfiguration,
        Filesystem $filesystem,
        MediaConfig $mediaConfig
    ) {
        parent::__construct($context);

        $this->encryptor                     = $encryptor;
        $this->serializer                    = $serializer;
        $this->eavConfig                     = $eavConfig;
        $this->storeManager                  = $storeManager;
        $this->mediaConfig                   = $mediaConfig;
        $this->catalogInventoryConfiguration = $catalogInventoryConfiguration;
        $this->mediaDirectory                = $filesystem->getDirectoryWrite(DirectoryList::MEDIA);
    }

    /**
     * Retrieve Akeneo base URL
     *
     * @return string
     */
    public function getAkeneoApiBaseUrl()
    {
        return $this->scopeConfig->getValue(self::AKENEO_API_BASE_URL);
    }

    /**
     * Retrieve Akeneo username
     *
     * @return string
     */
    public function getAkeneoApiUsername()
    {
        return $this->scopeConfig->getValue(self::AKENEO_API_USERNAME);
    }

    /**
     * Retrieve Akeneo password
     *
     * @return string
     */
    public function getAkeneoApiPassword()
    {
        /** @var string $password */
        $password = $this->scopeConfig->getValue(self::AKENEO_API_PASSWORD);

        return $this->encryptor->decrypt($password);
    }

    /**
     * Retrieve Akeneo client_id
     *
     * @return string
     */
    public function getAkeneoApiClientId()
    {
        return $this->scopeConfig->getValue(self::AKENEO_API_CLIENT_ID);
    }

    /**
     * Retrieve Akeneo client_secret
     *
     * @return string
     */
    public function getAkeneoApiClientSecret()
    {
        return $this->scopeConfig->getValue(self::AKENEO_API_CLIENT_SECRET);
    }

    /**
     * Check whether Akeneo is an enterprise or not
     *
     * @return bool
     */
    public function isAkeneoEnterprise()
    {
        return $this->scopeConfig->isSetFlag(self::AKENEO_API_IS_ENTERPRISE);
    }

    /**
     * Retrieve the filter mode used
     *
     * @see \Pimgento\Api\Model\Source\Filters\Mode
     *
     * @return string
     */
    public function getFilterMode()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_MODE);
    }

    /**
     * Retrieve the type of filter to apply on the completeness
     *
     * @see \Pimgento\Api\Model\Source\Filters\Completeness
     *
     * @return string
     */
    public function getCompletenessTypeFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_COMPLETENESS_TYPE);
    }

    /**
     * Retrieve the value to filter the completeness
     *
     * @return string
     */
    public function getCompletenessValueFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_COMPLETENESS_VALUE);
    }

    /**
     * Retrieve the scope to apply the completeness filter on
     *
     * @return string
     */
    public function getCompletenessScopeFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_COMPLETENESS_SCOPE);
    }

    /**
     * Retrieve the locales to apply the completeness filter on
     *
     * @return string
     */
    public function getCompletenessLocalesFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_COMPLETENESS_LOCALES);
    }

    /**
     * Retrieve the status filter
     *
     * @see \Pimgento\Api\Model\Source\Filters\Status
     *
     * @return string
     */
    public function getStatusFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_STATUS);
    }

    /**
     * Retrieve the updated filter
     *
     * @return string
     */
    public function getUpdatedFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_UPDATED);
    }

    /**
     * Retrieve the families to filter the products on
     *
     * @return string
     */
    public function getFamiliesFilter()
    {
        return $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_FAMILIES);
    }

    /**
     * Retrieve the advance filters
     *
     * @return array
     */
    public function getAdvancedFilters()
    {
        $filters = $this->scopeConfig->getValue(self::PRODUCTS_FILTERS_ADVANCED_FILTER);

        return $this->serializer->unserialize($filters);
    }

    /**
     * Retrieve website mapping
     *
     * @return string
     */
    public function getWebsiteMapping()
    {
        return $this->scopeConfig->getValue(self::AKENEO_API_WEBSITE_MAPPING);
    }

    /**
     * Retrieve default locale
     *
     * @param int $storeId
     *
     * @return string
     */
    public function getDefaultLocale($storeId = null)
    {
        return $this->scopeConfig->getValue(
            Data::XML_PATH_DEFAULT_LOCALE,
            ScopeInterface::SCOPE_STORE,
            $storeId
        );
    }

    /**
     * Retrieve default currency
     *
     * @param int $storeId
     *
     * @return string
     */
    public function getDefaultCurrency($storeId = null)
    {
        return $this->scopeConfig->getValue(
            Currency::XML_PATH_CURRENCY_DEFAULT,
            ScopeInterface::SCOPE_STORE,
            $storeId
        );
    }

    /**
     * Retrieve pagination size
     *
     * @return string|int
     */
    public function getPanigationSize()
    {
        /** @var string|int $paginationSize */
        $paginationSize = $this->scopeConfig->getValue(self::AKENEO_API_PAGINATION_SIZE);
        if (!$paginationSize) {
            $paginationSize = self::PAGINATION_SIZE_DEFAULT_VALUE;
        }

        return $paginationSize;
    }

    /**
     * Retrieve entity type id from entity name
     *
     * @param string $entity
     *
     * @return int
     * @throws \Magento\Framework\Exception\LocalizedException
     */
    public function getEntityTypeId($entity)
    {
        return $this->eavConfig->getEntityType($entity)->getEntityTypeId();
    }

    /**
     * Retrieve attribute by code
     *
     * @param string $entityType
     * @param string $code
     *
     * @return AbstractAttribute
     */
    public function getAttribute($entityType, $code)
    {
        return $this->eavConfig->getAttribute($entityType, $code);
    }

    /**
     * Retrieve stores default tax class
     *
     * @return array
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    public function getProductTaxClasses()
    {
        /** @var array $stores */
        $stores = $this->storeManager->getStores(true);
        /** @var array $result */
        $result = [];

        /** @var string|array $classes */
        $classes = $this->scopeConfig->getValue(self::PRODUCT_TAX_CLASS);
        if (!$classes) {
            return $result;
        }

        $classes = $this->serializer->unserialize($classes);
        if (!is_array($classes)) {
            return $result;
        }

        /** @var array $class */
        foreach ($classes as $class) {
            if (!isset($class['website'])) {
                continue;
            }
            if (!isset($class['tax_class'])) {
                continue;
            }

            if ($this->getDefaultWebsiteId() === $class['website']) {
                $result[0] = $class['tax_class'];
            }

            /** @var StoreInterface $store */
            foreach ($stores as $store) {
                if ($store->getWebsiteId() === $class['website']) {
                    $result[$store->getId()] = $class['tax_class'];
                }
            }
        }

        return $result;
    }

    /**
     * Retrieve default website id
     *
     * @return int
     * @throws \Magento\Framework\Exception\NoSuchEntityException
     */
    public function getDefaultWebsiteId()
    {
        return $this->storeManager->getStore()->getWebsiteId();
    }

    /**
     * Retrieve default scope id used by the catalog inventory module when saving an entity
     *
     * @return int
     */
    public function getDefaultScopeId()
    {
        return $this->catalogInventoryConfiguration->getDefaultScopeId();
    }

    /**
     * Description isMediaImportEnabled function
     *
     * @return bool
     */
    public function isMediaImportEnabled()
    {
        return $this->scopeConfig->isSetFlag(self::PRODUCT_MEDIA_ENABLED);
    }

    /**
     * Description isUrlGenerationEnabled function
     *
     * @return bool
     */
    public function isUrlGenerationEnabled()
    {
        return $this->scopeConfig->isSetFlag(self::PRODUCT_URL_GENERATION_ENABLED);
    }

    /**
     * Retrieve media attribute column
     *
     * @return array
     */
    public function getMediaImportImagesColumns()
    {
        /** @var array $images */
        $images = [];
        /** @var string $config */
        $config = $this->scopeConfig->getValue(self::PRODUCT_MEDIA_IMAGES);
        if (!$config) {
            return $images;
        }

        /** @var array $media */
        $media = $this->serializer->unserialize($config);
        if (!$media) {
            return $images;
        }

        return $media;
    }

    /**
     * Retrieve media attribute column
     *
     * @return array
     */
    public function getMediaImportGalleryColumns()
    {
        /** @var array $images */
        $images = [];
        /** @var string $config */
        $config = $this->scopeConfig->getValue(self::PRODUCT_MEDIA_GALLERY);
        if (!$config) {
            return $images;
        }

        /** @var array $media */
        $media = $this->serializer->unserialize($config);
        if (!$media) {
            return $images;
        }

        foreach ($media as $image) {
            if (!isset($image['attribute'])) {
                continue;
            }
            $images[] = $image['attribute'];
        }

        return $images;
    }

    /**
     * Retrieve is asset import is enabled
     *
     * @return bool
     */
    public function isAssetImportEnabled()
    {
        return $this->scopeConfig->isSetFlag(self::PRODUCT_ASSET_ENABLED);
    }

    /**
     * Retrieve asset attribute columns
     *
     * @return array
     */
    public function getAssetImportGalleryColumns()
    {
        /** @var array $assets */
        $assets = [];
        /** @var string $config */
        $config = $this->scopeConfig->getValue(self::PRODUCT_ASSET_GALLERY);
        if (!$config) {
            return $assets;
        }

        /** @var array $media */
        $media = $this->serializer->unserialize($config);
        if (!$media) {
            return $assets;
        }

        foreach ($media as $asset) {
            if (!isset($asset['attribute'])) {
                continue;
            }
            $assets[] = $asset['attribute'];
        }

        return $assets;
    }

    /**
     * Check if media file exists
     *
     * @param string $filename
     *
     * @return bool
     */
    public function mediaFileExists($filename)
    {
        return $this->mediaDirectory->isFile($this->mediaConfig->getMediaPath($this->getMediaFilePath($filename)));
    }

    /**
     * Retrieve media directory path
     *
     * @param string $filename
     * @param string $content
     *
     * @return void
     */
    public function saveMediaFile($filename, $content)
    {
        if (!$this->mediaFileExists($filename)) {
            $this->mediaDirectory->writeFile(
                $this->mediaConfig->getMediaPath($this->getMediaFilePath($filename)),
                $content
            );
        }
    }

    /**
     * Retrieve media file path
     *
     * @param string $filename
     *
     * @return string
     */
    public function getMediaFilePath($filename)
    {
        return Uploader::getDispretionPath($filename) . '/' . Uploader::getCorrectFileName($filename);
    }

    /**
     * Retrieve if category is used in product URL
     *
     * @param int $storeId
     *
     * @return bool
     */
    public function isCategoryUsedInProductUrl($storeId = null)
    {
        return $this->scopeConfig->isSetFlag(
            ProductHelper::XML_PATH_PRODUCT_URL_USE_CATEGORY,
            ScopeInterface::SCOPE_STORE,
            $storeId
        );
    }

    /**
     * Check if url_key attribute is mapped with PIM attribute
     *
     * @return bool
     */
    public function isUrlKeyMapped()
    {
        /** @var mixed $matches */
        $matches = $this->scopeConfig->getValue(self::PRODUCT_ATTRIBUTE_MAPPING_SIMPLE);
        /** @var mixed[] $matches */
        $matches = $this->serializer->unserialize($matches);
        if (!is_array($matches)) {
            return false;
        }

        /** @var mixed[] $match */
        foreach ($matches as $match) {
            if (!isset($match['pim_attribute'], $match['magento_attribute'])) {
                continue;
            }

            /** @var string $magentoAttribute */
            $magentoAttribute = $match['magento_attribute'];
            if ($magentoAttribute === 'url_key') {
                return true;
            }
        }

        return false;
    }
}
