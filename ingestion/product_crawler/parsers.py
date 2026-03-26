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


def extract_product_fields(react_data: dict) -> dict:
    """
    Extract product fields from react_data object.

    Extracts all simple/scalar fields. Skips complex nested structures like:
    - options (ring sizes - too large)
    - quick_options (nested variants)
    - associate (related products)
    - product_price (duplicate of price fields)

    Args:
        react_data: Parsed react_data dictionary

    Returns:
        dict: Extracted fields (all available simple fields)
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

        # Pricing
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

        # Store
        'store_code': react_data.get('store_code'),

        # Product flags/settings
        'platinum_palladium_info_in_alloy': react_data.get('platinum_palladium_info_in_alloy'),
        'bracelet_without_chain': react_data.get('bracelet_without_chain'),
        'show_popup_quantity_eternity': react_data.get('show_popup_quantity_eternity'),
        'visible_contents': react_data.get('visible_contents'),
        'gender': react_data.get('gender'),
        'configure_mode': react_data.get('configure_mode'),
        'included_chain_weight': react_data.get('included_chain_weight'),

        # Media
        'media_image': react_data.get('media_image'),
        'media_video': react_data.get('media_video'),

        # Additional fields
        'compare_sizes': react_data.get('compare_sizes'),
        'option_dependent': react_data.get('option_dependent'),
        'preconfigure': react_data.get('preconfigure'),
        'attributes': react_data.get('attributes'),
        'dimension_guide': react_data.get('dimension_guide'),
        'attributes_link': react_data.get('attributes_link'),
        'super_data': react_data.get('super_data'),
        'quantity_option': react_data.get('quantity_option'),
        'product_set': react_data.get('product_set'),
        'discount_custom_options': react_data.get('discount_custom_options'),
        'designProvider': react_data.get('designProvider'),
    }

    return result
