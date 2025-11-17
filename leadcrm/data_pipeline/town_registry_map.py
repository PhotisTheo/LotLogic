"""
Mapping of Massachusetts town IDs to their Registry of Deeds district.

Each town in MA has a unique MassGIS town ID (e.g., 2507000 for Boston).
This module maps those IDs to the correct registry district for scraping.

Registry Districts:
- Suffolk: Boston metro
- Essex North/South: North Shore
- Middlesex North/South: Cambridge/Newton area
- Norfolk: South Shore
- Plymouth: Plymouth County
- Worcester North/South: Worcester County
- Barnstable: Cape Cod
- Bristol North/South: Southeastern MA
- Hampden: Springfield area
- Hampshire: Northampton/Amherst
- Franklin: Franklin County
- Berkshire North/Middle/South: Berkshires
- Dukes: Martha's Vineyard
- Nantucket: Nantucket Island
"""

from typing import Dict, Optional

# Simple town ID to Registry ID mapping (for MassGIS integer IDs)
# These are the short integer IDs used in the MassGIS catalog
SIMPLE_TOWN_TO_REGISTRY: Dict[int, str] = {
    # Essex North District (14 towns)
    7: "essex_north",     # Amesbury
    9: "essex_north",     # Andover
    38: "essex_north",    # Boxford
    105: "essex_north",   # Georgetown
    116: "essex_north",   # Groveland
    128: "essex_north",   # Haverhill
    149: "essex_north",   # Lawrence
    180: "essex_north",   # Merrimac
    181: "essex_north",   # Methuen
    206: "essex_north",   # Newburyport
    210: "essex_north",   # North Andover
    254: "essex_north",   # Rowley
    259: "essex_north",   # Salisbury
    324: "essex_north",   # West Newbury

    # Essex South District (17 towns)
    30: "essex_south",    # Beverly
    71: "essex_south",    # Danvers
    92: "essex_south",    # Essex
    107: "essex_south",   # Gloucester
    119: "essex_south",   # Hamilton
    163: "essex_south",   # Lynn
    166: "essex_south",   # Manchester
    168: "essex_south",   # Marblehead
    184: "essex_south",   # Middleton
    196: "essex_south",   # Nahant
    229: "essex_south",   # Peabody
    252: "essex_south",   # Rockport
    258: "essex_south",   # Salem
    262: "essex_south",   # Saugus
    291: "essex_south",   # Swampscott
    298: "essex_south",   # Topsfield
    320: "essex_south",   # Wenham
}

# Full town ID to Registry ID mapping (legacy format)
# Format: {town_id: registry_id}
TOWN_TO_REGISTRY: Dict[str, str] = {
    # Suffolk County
    "2507000": "suffolk",  # Boston
    "2507030": "suffolk",  # Chelsea
    "2507280": "suffolk",  # Revere
    "2507340": "suffolk",  # Winthrop

    # Essex North District (Lawrence/Haverhill area)
    "2509000": "essex_north",  # Lawrence
    "2509010": "essex_north",  # Haverhill
    "2509020": "essex_north",  # Methuen
    "2509030": "essex_north",  # Andover
    "2509040": "essex_north",  # North Andover
    "2509050": "essex_north",  # Amesbury
    "2509060": "essex_north",  # Newburyport
    "2509070": "essex_north",  # Salisbury
    "2509080": "essex_north",  # Merrimac
    "2509090": "essex_north",  # West Newbury
    "2509100": "essex_north",  # Groveland
    "2509110": "essex_north",  # Georgetown
    "2509120": "essex_north",  # Boxford
    "2509130": "essex_north",  # Rowley

    # Essex South District (Salem/Lynn area)
    "2509140": "essex_south",  # Salem
    "2509150": "essex_south",  # Lynn
    "2509160": "essex_south",  # Peabody
    "2509170": "essex_south",  # Beverly
    "2509180": "essex_south",  # Marblehead
    "2509190": "essex_south",  # Swampscott
    "2509200": "essex_south",  # Nahant
    "2509210": "essex_south",  # Danvers
    "2509220": "essex_south",  # Middleton
    "2509230": "essex_south",  # Topsfield
    "2509240": "essex_south",  # Wenham
    "2509250": "essex_south",  # Hamilton
    "2509260": "essex_south",  # Essex
    "2509270": "essex_south",  # Gloucester
    "2509280": "essex_south",  # Rockport
    "2509290": "essex_south",  # Manchester-by-the-Sea
    "2509300": "essex_south",  # Saugus

    # Middlesex North District (Lowell/Concord area)
    "2517000": "middlesex_north",  # Lowell
    "2517010": "middlesex_north",  # Cambridge (Northern part uses North)
    "2517020": "middlesex_north",  # Somerville
    "2517030": "middlesex_north",  # Medford
    "2517040": "middlesex_north",  # Malden
    "2517050": "middlesex_north",  # Everett
    "2517060": "middlesex_north",  # Melrose
    "2517070": "middlesex_north",  # Wakefield
    "2517080": "middlesex_north",  # Reading
    "2517090": "middlesex_north",  # Stoneham
    "2517100": "middlesex_north",  # Woburn
    "2517110": "middlesex_north",  # Winchester
    "2517120": "middlesex_north",  # Arlington
    "2517130": "middlesex_north",  # Belmont
    "2517140": "middlesex_north",  # Lexington
    "2517150": "middlesex_north",  # Burlington
    "2517160": "middlesex_north",  # Bedford
    "2517170": "middlesex_north",  # Concord
    "2517180": "middlesex_north",  # Carlisle
    "2517190": "middlesex_north",  # Chelmsford
    "2517200": "middlesex_north",  # Billerica
    "2517210": "middlesex_north",  # Tewksbury
    "2517220": "middlesex_north",  # Wilmington
    "2517230": "middlesex_north",  # Tyngsborough
    "2517240": "middlesex_north",  # Dracut
    "2517250": "middlesex_north",  # Westford
    "2517260": "middlesex_north",  # Littleton
    "2517270": "middlesex_north",  # Acton
    "2517280": "middlesex_north",  # Maynard
    "2517290": "middlesex_north",  # Stow
    "2517300": "middlesex_north",  # Hudson
    "2517310": "middlesex_north",  # Marlborough
    "2517320": "middlesex_north",  # Boxborough
    "2517330": "middlesex_north",  # Lincoln
    "2517340": "middlesex_north",  # Sudbury
    "2517350": "middlesex_north",  # Wayland
    "2517360": "middlesex_north",  # Weston
    "2517370": "middlesex_north",  # Waltham
    "2517380": "middlesex_north",  # Watertown
    "2517390": "middlesex_north",  # Newton (split - using North for now)
    "2517400": "middlesex_north",  # Waltham
    "2517410": "middlesex_north",  # Lincoln
    "2517420": "middlesex_north",  # Pepperell
    "2517430": "middlesex_north",  # Groton
    "2517440": "middlesex_north",  # Dunstable
    "2517450": "middlesex_north",  # Ayer
    "2517460": "middlesex_north",  # Shirley
    "2517470": "middlesex_north",  # Townsend
    "2517480": "middlesex_north",  # Ashby

    # Middlesex South District (Framingham/Natick area)
    "2517500": "middlesex_south",  # Framingham
    "2517510": "middlesex_south",  # Natick
    "2517520": "middlesex_south",  # Ashland
    "2517530": "middlesex_south",  # Holliston
    "2517540": "middlesex_south",  # Hopkinton
    "2517550": "middlesex_south",  # Sherborn
    "2517560": "middlesex_south",  # Medfield
    "2517570": "middlesex_south",  # Millis
    "2517580": "middlesex_south",  # Bellingham
    "2517590": "middlesex_south",  # Newton (Southern part)

    # Norfolk County
    "2521000": "norfolk",  # Dedham
    "2521010": "norfolk",  # Quincy
    "2521020": "norfolk",  # Braintree
    "2521030": "norfolk",  # Weymouth
    "2521040": "norfolk",  # Milton
    "2521050": "norfolk",  # Brookline
    "2521060": "norfolk",  # Needham
    "2521070": "norfolk",  # Wellesley
    "2521080": "norfolk",  # Dover
    "2521090": "norfolk",  # Westwood
    "2521100": "norfolk",  # Norwood
    "2521110": "norfolk",  # Walpole
    "2521120": "norfolk",  # Sharon
    "2521130": "norfolk",  # Canton
    "2521140": "norfolk",  # Stoughton
    "2521150": "norfolk",  # Randolph
    "2521160": "norfolk",  # Holbrook
    "2521170": "norfolk",  # Avon
    "2521180": "norfolk",  # Norwood
    "2521190": "norfolk",  # Foxborough
    "2521200": "norfolk",  # Franklin
    "2521210": "norfolk",  # Bellingham
    "2521220": "norfolk",  # Medway
    "2521230": "norfolk",  # Millis
    "2521240": "norfolk",  # Norfolk
    "2521250": "norfolk",  # Wrentham
    "2521260": "norfolk",  # Plainville

    # Plymouth County
    "2523000": "plymouth",  # Plymouth
    "2523010": "plymouth",  # Brockton
    "2523020": "plymouth",  # Abington
    "2523030": "plymouth",  # Bridgewater
    "2523040": "plymouth",  # East Bridgewater
    "2523050": "plymouth",  # West Bridgewater
    "2523060": "plymouth",  # Whitman
    "2523070": "plymouth",  # Hanson
    "2523080": "plymouth",  # Halifax
    "2523090": "plymouth",  # Kingston
    "2523100": "plymouth",  # Pembroke
    "2523110": "plymouth",  # Hanover
    "2523120": "plymouth",  # Norwell
    "2523130": "plymouth",  # Rockland
    "2523140": "plymouth",  # Hingham
    "2523150": "plymouth",  # Hull
    "2523160": "plymouth",  # Cohasset
    "2523170": "plymouth",  # Scituate
    "2523180": "plymouth",  # Marshfield
    "2523190": "plymouth",  # Duxbury
    "2523200": "plymouth",  # Carver
    "2523210": "plymouth",  # Wareham
    "2523220": "plymouth",  # Marion
    "2523230": "plymouth",  # Mattapoisett
    "2523240": "plymouth",  # Middleborough
    "2523250": "plymouth",  # Lakeville

    # Worcester North District
    "2527000": "worcester_north",  # Worcester (Northern part)
    "2527010": "worcester_north",  # Fitchburg
    "2527020": "worcester_north",  # Leominster
    "2527030": "worcester_north",  # Gardner
    "2527040": "worcester_north",  # Clinton
    "2527050": "worcester_north",  # Athol
    "2527060": "worcester_north",  # Orange
    "2527070": "worcester_north",  # Winchendon
    "2527080": "worcester_north",  # Templeton
    "2527090": "worcester_north",  # Westminster
    "2527100": "worcester_north",  # Ashburnham
    "2527110": "worcester_north",  # Lunenburg
    "2527120": "worcester_north",  # Sterling
    "2527130": "worcester_north",  # Lancaster
    "2527140": "worcester_north",  # Bolton
    "2527150": "worcester_north",  # Berlin
    "2527160": "worcester_north",  # Boylston

    # Worcester South District
    "2527200": "worcester_south",  # Worcester (Southern part)
    "2527210": "worcester_south",  # Auburn
    "2527220": "worcester_south",  # Leicester
    "2527230": "worcester_south",  # Spencer
    "2527240": "worcester_south",  # Charlton
    "2527250": "worcester_south",  # Southbridge
    "2527260": "worcester_south",  # Sturbridge
    "2527270": "worcester_south",  # Webster
    "2527280": "worcester_south",  # Dudley
    "2527290": "worcester_south",  # Oxford
    "2527300": "worcester_south",  # Millbury
    "2527310": "worcester_south",  # Grafton
    "2527320": "worcester_south",  # Shrewsbury
    "2527330": "worcester_south",  # Westborough
    "2527340": "worcester_south",  # Northborough
    "2527350": "worcester_south",  # Southborough

    # Barnstable County (Cape Cod)
    "2501000": "barnstable",  # Barnstable
    "2501010": "barnstable",  # Bourne
    "2501020": "barnstable",  # Brewster
    "2501030": "barnstable",  # Chatham
    "2501040": "barnstable",  # Dennis
    "2501050": "barnstable",  # Eastham
    "2501060": "barnstable",  # Falmouth
    "2501070": "barnstable",  # Harwich
    "2501080": "barnstable",  # Mashpee
    "2501090": "barnstable",  # Orleans
    "2501100": "barnstable",  # Provincetown
    "2501110": "barnstable",  # Sandwich
    "2501120": "barnstable",  # Truro
    "2501130": "barnstable",  # Wellfleet
    "2501140": "barnstable",  # Yarmouth

    # Bristol North District
    "2505000": "bristol_north",  # Attleboro
    "2505010": "bristol_north",  # Norton
    "2505020": "bristol_north",  # Mansfield
    "2505030": "bristol_north",  # Easton
    "2505040": "bristol_north",  # Raynham
    "2505050": "bristol_north",  # Taunton
    "2505060": "bristol_north",  # Rehoboth
    "2505070": "bristol_north",  # Seekonk
    "2505080": "bristol_north",  # Swansea
    "2505090": "bristol_north",  # Somerset
    "2505100": "bristol_north",  # Dighton
    "2505110": "bristol_north",  # Berkley

    # Bristol South District
    "2505200": "bristol_south",  # Fall River
    "2505210": "bristol_south",  # New Bedford
    "2505220": "bristol_south",  # Dartmouth
    "2505230": "bristol_south",  # Westport
    "2505240": "bristol_south",  # Fairhaven
    "2505250": "bristol_south",  # Acushnet
    "2505260": "bristol_south",  # Freetown

    # Hampden County
    "2513000": "hampden",  # Springfield
    "2513010": "hampden",  # Chicopee
    "2513020": "hampden",  # Holyoke
    "2513030": "hampden",  # Westfield
    "2513040": "hampden",  # West Springfield
    "2513050": "hampden",  # Agawam
    "2513060": "hampden",  # Longmeadow
    "2513070": "hampden",  # East Longmeadow
    "2513080": "hampden",  # Ludlow
    "2513090": "hampden",  # Wilbraham
    "2513100": "hampden",  # Palmer
    "2513110": "hampden",  # Monson
    "2513120": "hampden",  # Hampden
    "2513130": "hampden",  # Brimfield
    "2513140": "hampden",  # Wales
    "2513150": "hampden",  # Holland

    # Hampshire County
    "2515000": "hampshire",  # Northampton
    "2515010": "hampshire",  # Amherst
    "2515020": "hampshire",  # South Hadley
    "2515030": "hampshire",  # Easthampton
    "2515040": "hampshire",  # Hadley
    "2515050": "hampshire",  # Ware
    "2515060": "hampshire",  # Belchertown
    "2515070": "hampshire",  # Granby
    "2515080": "hampshire",  # Southampton
    "2515090": "hampshire",  # Williamsburg
    "2515100": "hampshire",  # Hatfield
    "2515110": "hampshire",  # Westhampton
    "2515120": "hampshire",  # Chesterfield
    "2515130": "hampshire",  # Cummington
    "2515140": "hampshire",  # Goshen
    "2515150": "hampshire",  # Middlefield
    "2515160": "hampshire",  # Plainfield
    "2515170": "hampshire",  # Worthington

    # Franklin County
    "2511000": "franklin",  # Greenfield
    "2511010": "franklin",  # Orange
    "2511020": "franklin",  # Montague
    "2511030": "franklin",  # Shelburne
    "2511040": "franklin",  # Deerfield
    "2511050": "franklin",  # Sunderland
    "2511060": "franklin",  # Whately
    "2511070": "franklin",  # Conway
    "2511080": "franklin",  # Ashfield
    "2511090": "franklin",  # Buckland
    "2511100": "franklin",  # Charlemont
    "2511110": "franklin",  # Colrain
    "2511120": "franklin",  # Gill
    "2511130": "franklin",  # Hawley
    "2511140": "franklin",  # Heath
    "2511150": "franklin",  # Leverett
    "2511160": "franklin",  # Leyden
    "2511170": "franklin",  # Monroe
    "2511180": "franklin",  # New Salem
    "2511190": "franklin",  # Northfield
    "2511200": "franklin",  # Rowe
    "2511210": "franklin",  # Shutesbury
    "2511220": "franklin",  # Warwick
    "2511230": "franklin",  # Wendell

    # Berkshire North District
    "2503000": "berkshire_north",  # North Adams
    "2503010": "berkshire_north",  # Williamstown
    "2503020": "berkshire_north",  # Adams
    "2503030": "berkshire_north",  # Cheshire
    "2503040": "berkshire_north",  # Clarksburg
    "2503050": "berkshire_north",  # Florida
    "2503060": "berkshire_north",  # Savoy

    # Berkshire Middle District
    "2503100": "berkshire_middle",  # Pittsfield
    "2503110": "berkshire_middle",  # Dalton
    "2503120": "berkshire_middle",  # Lanesborough
    "2503130": "berkshire_middle",  # Hancock
    "2503140": "berkshire_middle",  # Richmond
    "2503150": "berkshire_middle",  # Lenox
    "2503160": "berkshire_middle",  # Washington
    "2503170": "berkshire_middle",  # Hinsdale
    "2503180": "berkshire_middle",  # Peru
    "2503190": "berkshire_middle",  # Windsor
    "2503200": "berkshire_middle",  # Cummington
    "2503210": "berkshire_middle",  # Worthington

    # Berkshire South District
    "2503300": "berkshire_south",  # Great Barrington
    "2503310": "berkshire_south",  # Stockbridge
    "2503320": "berkshire_south",  # Lee
    "2503330": "berkshire_south",  # Becket
    "2503340": "berkshire_south",  # Otis
    "2503350": "berkshire_south",  # Tyringham
    "2503360": "berkshire_south",  # Monterey
    "2503370": "berkshire_south",  # Sandisfield
    "2503380": "berkshire_south",  # New Marlborough
    "2503390": "berkshire_south",  # Sheffield
    "2503400": "berkshire_south",  # Egremont
    "2503410": "berkshire_south",  # Mount Washington
    "2503420": "berkshire_south",  # Alford
    "2503430": "berkshire_south",  # West Stockbridge

    # Dukes County (Martha's Vineyard)
    "2507500": "dukes",  # Edgartown
    "2507510": "dukes",  # Oak Bluffs
    "2507520": "dukes",  # Tisbury
    "2507530": "dukes",  # West Tisbury
    "2507540": "dukes",  # Chilmark
    "2507550": "dukes",  # Aquinnah

    # Nantucket County
    "2519000": "nantucket",  # Nantucket
}


def get_registry_for_town(town_id) -> Optional[str]:
    """
    Get the registry district ID for a given Massachusetts town ID.

    Args:
        town_id: MassGIS town ID - can be:
                 - Integer: 206 for Newburyport (MassGIS catalog format)
                 - String: "206"
                 - Full string format: "2507000" for Boston (legacy)

    Returns:
        Registry ID string (e.g., "essex_north") or None if not found

    Example:
        >>> get_registry_for_town(206)
        'essex_north'
        >>> get_registry_for_town("206")
        'essex_north'
        >>> get_registry_for_town("2507000")
        'suffolk'
    """
    # Try simple integer mapping first (MassGIS catalog IDs)
    if isinstance(town_id, int):
        if town_id in SIMPLE_TOWN_TO_REGISTRY:
            return SIMPLE_TOWN_TO_REGISTRY[town_id]
        # Convert to string for legacy lookup
        town_id = str(town_id)

    # Try string integer mapping
    if isinstance(town_id, str) and town_id.isdigit():
        town_id_int = int(town_id)
        if town_id_int in SIMPLE_TOWN_TO_REGISTRY:
            return SIMPLE_TOWN_TO_REGISTRY[town_id_int]

    # Try legacy full format (e.g., "2507000")
    if town_id in TOWN_TO_REGISTRY:
        return TOWN_TO_REGISTRY[town_id]

    return None


def get_all_registries() -> list[str]:
    """
    Get a list of all unique registry IDs.

    Returns:
        Sorted list of registry IDs
    """
    return sorted(set(TOWN_TO_REGISTRY.values()))


def get_towns_for_registry(registry_id: str) -> list[str]:
    """
    Get all town IDs that belong to a given registry district.

    Args:
        registry_id: Registry district ID (e.g., "suffolk")

    Returns:
        List of town IDs in that registry district
    """
    return [town_id for town_id, reg_id in TOWN_TO_REGISTRY.items() if reg_id == registry_id]
