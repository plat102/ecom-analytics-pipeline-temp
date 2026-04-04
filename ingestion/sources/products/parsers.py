"""
Data parsers for product crawling.

Supports multiple parsing strategies:
- BeautifulSoup-based HTML parsing (for Playwright-rendered pages)
- Regex + JSON parsing (for server-side rendered react_data)
"""

import json
import re

from bs4 import BeautifulSoup

from common.utils.logger import get_logger

logger = get_logger(__name__)


def parse_product_name(html: str) -> str | None:
    """
    Extract product name from HTML using BeautifulSoup with fallback selectors.

    Strategy:
    1. Primary: og:title meta tag
    2. Fallback 1: h1 span
    3. Fallback 2: any h1 tag

    Args:
        html: HTML content as string

    Returns:
        str | None: Product name or None if not found
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Primary: Open Graph title
    og_title = soup.find('meta', property='og:title')
    if og_title and og_title.get('content'):
        return og_title['content'].strip()

    # Fallback 1: h1 span
    h1_span = soup.select_one('h1 span')
    if h1_span:
        return h1_span.get_text(strip=True)

    # Fallback 2: any h1 tag
    h1_tag = soup.find('h1')
    if h1_tag:
        return h1_tag.get_text(strip=True)

    return None


def extract_react_data(html: str) -> dict | None:
    """
    Extract react_data JavaScript object from HTML using regex.

    Pattern: var react_data = {...};

    This is typically found in server-side rendered Glamira pages.

    Args:
        html: HTML content as string

    Returns:
        dict: Parsed react_data or None if not found/invalid JSON
    """
    pattern = r'var\s+react_data\s*=\s*({.*?});'
    match = re.search(pattern, html, re.DOTALL)

    if match:
        try:
            json_str = match.group(1)
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse react_data JSON: {e}")
            return None

    return None


def extract_basic_fields_from_html(html: str) -> dict | None:
    """
    Fallback parser when react_data not found.
    Extracts product data from JSON-LD structured data (Google SEO standard for indexing).

    Note:
        Marked with 'data_source': 'json_ld_fallback' for tracking.
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        script = soup.find('script', id='structured-data', type='application/ld+json')

        if script and script.string:
            structured_data = json.loads(script.string)
            product_data = structured_data.get('mainEntity', {})

            if product_data:
                # Extract offers data
                offers = product_data.get('offers', {})

                # Extract additional properties (stone, metal info)
                additional_props = {}
                for prop in product_data.get('additionalProperty', []):
                    if isinstance(prop, dict):
                        name = prop.get('name', '')
                        value = prop.get('value', '')
                        if name and value:
                            additional_props[name.lower()] = value

                result = {
                    'product_name': product_data.get('name'),
                    'sku': product_data.get('sku'),
                    'category_name': product_data.get('category'),
                    'price': offers.get('price'),
                    'currency_code': offers.get('priceCurrency'),
                    'availability': offers.get('availability'),
                    'brand': product_data.get('brand', {}).get('name') if isinstance(product_data.get('brand'), dict) else None,
                    'data_source': 'json_ld_fallback',
                    'note': 'Fallback: JSON-LD structured data'
                }

                # Add additional properties if available
                if additional_props:
                    result['additional_properties'] = additional_props

                # Must have at least name and sku
                if result['product_name'] and result['sku']:
                    logger.info("Fallback parser: JSON-LD structured data succeeded")
                    return result

    except (json.JSONDecodeError, AttributeError, KeyError) as e:
        logger.warning(f"JSON-LD fallback parser failed: {e}")

    return None


def extract_product_fields(react_data: dict) -> dict:
    """
    Extract product fields from react_data object for analytics.

    Focuses on fields with high analytical value. Includes:
    - Core product info (ID, name, SKU, classification)
    - Pricing with currency info
    - Material properties (weights, flags)
    - Simplified media (primary image/video only)
    - Product flags from mentor's examples

    Excludes:
    - Large nested structures (full media arrays, dimension_guide, compare_sizes)
    - Technical configs (option_dependent, preconfigure)
    - Usually empty fields (super_data, designProvider, product_set)

    Args:
        react_data: Parsed react_data dictionary

    Returns:
        dict: Extracted fields optimized for analytics (~35 fields)
    """
    result = {
        # Product identification
        'product_id': react_data.get('product_id'),
        'product_name': react_data.get('name'),
        'sku': react_data.get('sku'),

        # Product classification
        'attribute_set_id': react_data.get('attribute_set_id'),
        'attribute_set': react_data.get('attribute_set'),
        'type_id': react_data.get('type_id'),
        'product_type': react_data.get('product_type'),
        'product_type_value': react_data.get('product_type_value'),

        # Pricing (keep both raw and formatted for currency info)
        'price': react_data.get('price'),
        'min_price': react_data.get('min_price'),
        'max_price': react_data.get('max_price'),
        'min_price_format': react_data.get('min_price_format'),
        'max_price_format': react_data.get('max_price_format'),

        # Material properties
        'gold_weight': react_data.get('gold_weight'),
        'none_metal_weight': react_data.get('none_metal_weight'),
        'fixed_silver_weight': react_data.get('fixed_silver_weight'),
        'material_design': react_data.get('material_design'),

        # Quantity
        'qty': react_data.get('qty'),

        # Collection
        'collection': react_data.get('collection'),
        'collection_id': react_data.get('collection_id'),

        # Category
        'category': react_data.get('category'),
        'category_name': react_data.get('category_name'),

        # Store & Demographics
        'store_code': react_data.get('store_code'),
        'gender': react_data.get('gender'),

        # Product flags (from mentor's examples)
        'platinum_palladium_info_in_alloy': react_data.get('platinum_palladium_info_in_alloy'),
        'bracelet_without_chain': react_data.get('bracelet_without_chain'),
        'show_popup_quantity_eternity': react_data.get('show_popup_quantity_eternity'),
        'visible_contents': react_data.get('visible_contents'),
        'configure_mode': react_data.get('configure_mode'),
        'included_chain_weight': react_data.get('included_chain_weight'),
    }

    # Extract currency code and tax rate from product_price
    product_price = react_data.get('product_price')
    if product_price and isinstance(product_price, dict):
        result['currency_code'] = product_price.get('currencyCode')
        result['tax_rate'] = product_price.get('currentTax')
    else:
        result['currency_code'] = None
        result['tax_rate'] = None

    # Extract primary image URL (simplified)
    media_image = react_data.get('media_image')
    if media_image and isinstance(media_image, dict):
        images = media_image.get('images', [])
        if images and len(images) > 0:
            result['primary_image_url'] = images[0].get('large_image_url')
        else:
            result['primary_image_url'] = None
    else:
        result['primary_image_url'] = None

    # Extract main video URL (simplified)
    media_video = react_data.get('media_video')
    if media_video and isinstance(media_video, dict):
        videos = media_video.get('videos', [])
        if videos and len(videos) > 0:
            result['primary_video_url'] = videos[0].get('url')
        else:
            result['primary_video_url'] = None
    else:
        result['primary_video_url'] = None

    # Extract key attributes only
    attributes = react_data.get('attributes')
    if attributes and isinstance(attributes, dict):
        # Gender from attributes (as backup if not in root)
        if 'gender' in attributes:
            gender_attr = attributes['gender']
            if isinstance(gender_attr, dict):
                result['gender_label'] = gender_attr.get('value')

        # Massiv/Solid indicator
        if 'massiv' in attributes:
            massiv_attr = attributes['massiv']
            if isinstance(massiv_attr, dict):
                result['is_solid'] = massiv_attr.get('value')

    return result


def process_html_to_product(html: str, product_id: str, url: str) -> dict:
    """
    Unified HTML processing: parse HTML and extract product data.

    Facade function that orchestrates the entire parsing workflow:
    1. Try react_data extraction (primary method)
    2. Fallback to JSON-LD parser (for edge cases)

    This is the ONLY function that external modules (crawler, retry) should use.

    Args:
        html: Raw HTML content
        product_id: Product ID (database source of truth)
        url: Product URL

    Returns:
        dict: Product data with status ("success", "no_react_data", or "error")
    """
    result = {
        "product_id": product_id,
        "url": url,
        "status": "error",
        "error_message": None
    }

    # Try react_data extraction (primary method)
    react_data = extract_react_data(html)

    if react_data:
        result["status"] = "success"
        fields = extract_product_fields(react_data)
        result.update(fields)
        # Preserve input product_id (database ID is source of truth)
        result["product_id"] = product_id
        return result

    # Try JSON-LD fallback parser (for edge cases)
    basic_fields = extract_basic_fields_from_html(html)

    if basic_fields:
        result["status"] = "success"
        result.update(basic_fields)
        result["product_id"] = product_id
        logger.info(f"Product {product_id}: Recovered via HTML fallback parser")
        return result

    # Both parsers failed
    result["status"] = "no_react_data"
    result["error_message"] = "react_data not found, HTML fallback also failed"
    return result
