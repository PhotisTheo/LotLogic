"""
Mapping of New Hampshire municipalities to their Registry of Deeds county.

Each municipality in NH belongs to one of 10 county registries.
This module maps municipality names to the correct registry for scraping.

Registry Counties:
- Rockingham: Seacoast area (Portsmouth, Salem, Derry, etc.)
- Strafford: Dover, Rochester area
- Belknap: Lakes Region (Laconia, Meredith, etc.)
- Carroll: Mt. Washington Valley (Conway, Wolfeboro, etc.)
- Merrimack: Concord, Franklin area
- Hillsborough: Manchester, Nashua area
- Cheshire: Keene area
- Sullivan: Claremont, Newport area
- Grafton: Lebanon, Hanover area
- Coos: Berlin, Northern NH
"""

from typing import Dict, Optional

# NH Municipality to Registry County mapping
# Format: {municipality_name: registry_county}
NH_TOWN_TO_REGISTRY: Dict[str, str] = {
    # Rockingham County (38 municipalities)
    "Atkinson": "rockingham",
    "Auburn": "rockingham",
    "Brentwood": "rockingham",
    "Candia": "rockingham",
    "Chester": "rockingham",
    "Danville": "rockingham",
    "Deerfield": "rockingham",
    "Derry": "rockingham",
    "East Kingston": "rockingham",
    "Epping": "rockingham",
    "Exeter": "rockingham",
    "Fremont": "rockingham",
    "Greenland": "rockingham",
    "Hampstead": "rockingham",
    "Hampton": "rockingham",
    "Hampton Falls": "rockingham",
    "Kensington": "rockingham",
    "Kingston": "rockingham",
    "Londonderry": "rockingham",
    "New Castle": "rockingham",
    "Newfields": "rockingham",
    "Newington": "rockingham",
    "Newmarket": "rockingham",
    "Newton": "rockingham",
    "North Hampton": "rockingham",
    "Northwood": "rockingham",
    "Nottingham": "rockingham",
    "Plaistow": "rockingham",
    "Portsmouth": "rockingham",
    "Raymond": "rockingham",
    "Rye": "rockingham",
    "Salem": "rockingham",
    "Sandown": "rockingham",
    "Seabrook": "rockingham",
    "South Hampton": "rockingham",
    "Stratham": "rockingham",
    "Windham": "rockingham",
    "Pelham": "rockingham",

    # Strafford County (13 municipalities)
    "Barrington": "strafford",
    "Dover": "strafford",
    "Durham": "strafford",
    "Farmington": "strafford",
    "Lee": "strafford",
    "Madbury": "strafford",
    "Middleton": "strafford",
    "Milton": "strafford",
    "New Durham": "strafford",
    "Rochester": "strafford",
    "Rollinsford": "strafford",
    "Somersworth": "strafford",
    "Strafford": "strafford",

    # Belknap County (11 municipalities)
    "Alton": "belknap",
    "Barnstead": "belknap",
    "Belmont": "belknap",
    "Center Harbor": "belknap",
    "Gilford": "belknap",
    "Gilmanton": "belknap",
    "Laconia": "belknap",
    "Meredith": "belknap",
    "New Hampton": "belknap",
    "Sanbornton": "belknap",
    "Tilton": "belknap",

    # Carroll County (18 municipalities)
    "Albany": "carroll",
    "Bartlett": "carroll",
    "Brookfield": "carroll",
    "Chatham": "carroll",
    "Conway": "carroll",
    "Eaton": "carroll",
    "Effingham": "carroll",
    "Freedom": "carroll",
    "Hart's Location": "carroll",
    "Jackson": "carroll",
    "Madison": "carroll",
    "Moultonborough": "carroll",
    "Ossipee": "carroll",
    "Sandwich": "carroll",
    "Tamworth": "carroll",
    "Tuftonboro": "carroll",
    "Wakefield": "carroll",
    "Wolfeboro": "carroll",

    # Merrimack County (27 municipalities)
    "Allenstown": "merrimack",
    "Andover": "merrimack",
    "Boscawen": "merrimack",
    "Bow": "merrimack",
    "Bradford": "merrimack",
    "Canterbury": "merrimack",
    "Chichester": "merrimack",
    "Concord": "merrimack",
    "Danbury": "merrimack",
    "Dunbarton": "merrimack",
    "Epsom": "merrimack",
    "Franklin": "merrimack",
    "Henniker": "merrimack",
    "Hill": "merrimack",
    "Hooksett": "merrimack",
    "Hopkinton": "merrimack",
    "Loudon": "merrimack",
    "Newbury": "merrimack",
    "New London": "merrimack",
    "Northfield": "merrimack",
    "Pembroke": "merrimack",
    "Pittsfield": "merrimack",
    "Salisbury": "merrimack",
    "Sutton": "merrimack",
    "Warner": "merrimack",
    "Webster": "merrimack",
    "Wilmot": "merrimack",

    # Hillsborough County (34 municipalities)
    "Amherst": "hillsborough",
    "Antrim": "hillsborough",
    "Bedford": "hillsborough",
    "Bennington": "hillsborough",
    "Brookline": "hillsborough",
    "Deering": "hillsborough",
    "Francestown": "hillsborough",
    "Goffstown": "hillsborough",
    "Greenfield": "hillsborough",
    "Greenville": "hillsborough",
    "Hancock": "hillsborough",
    "Hillsborough": "hillsborough",
    "Hollis": "hillsborough",
    "Hudson": "hillsborough",
    "Litchfield": "hillsborough",
    "Lyndeborough": "hillsborough",
    "Manchester": "hillsborough",
    "Mason": "hillsborough",
    "Merrimack": "hillsborough",
    "Milford": "hillsborough",
    "Mont Vernon": "hillsborough",
    "Nashua": "hillsborough",
    "New Boston": "hillsborough",
    "New Ipswich": "hillsborough",
    "Pelham": "hillsborough",
    "Peterborough": "hillsborough",
    "Sharon": "hillsborough",
    "Temple": "hillsborough",
    "Weare": "hillsborough",
    "Wilton": "hillsborough",
    "Windsor": "hillsborough",

    # Cheshire County (23 municipalities)
    "Alstead": "cheshire",
    "Chesterfield": "cheshire",
    "Dublin": "cheshire",
    "Fitzwilliam": "cheshire",
    "Gilsum": "cheshire",
    "Harrisville": "cheshire",
    "Hinsdale": "cheshire",
    "Jaffrey": "cheshire",
    "Keene": "cheshire",
    "Marlborough": "cheshire",
    "Marlow": "cheshire",
    "Nelson": "cheshire",
    "Richmond": "cheshire",
    "Rindge": "cheshire",
    "Roxbury": "cheshire",
    "Stoddard": "cheshire",
    "Sullivan": "cheshire",
    "Surry": "cheshire",
    "Swanzey": "cheshire",
    "Troy": "cheshire",
    "Walpole": "cheshire",
    "Westmoreland": "cheshire",
    "Winchester": "cheshire",

    # Sullivan County (15 municipalities)
    "Acworth": "sullivan",
    "Charlestown": "sullivan",
    "Claremont": "sullivan",
    "Cornish": "sullivan",
    "Croydon": "sullivan",
    "Goshen": "sullivan",
    "Grantham": "sullivan",
    "Langdon": "sullivan",
    "Lempster": "sullivan",
    "Newport": "sullivan",
    "Plainfield": "sullivan",
    "Springfield": "sullivan",
    "Sunapee": "sullivan",
    "Unity": "sullivan",
    "Washington": "sullivan",

    # Grafton County (39 municipalities)
    "Alexandria": "grafton",
    "Ashland": "grafton",
    "Bath": "grafton",
    "Benton": "grafton",
    "Bethlehem": "grafton",
    "Bridgewater": "grafton",
    "Bristol": "grafton",
    "Campton": "grafton",
    "Canaan": "grafton",
    "Dorchester": "grafton",
    "Easton": "grafton",
    "Ellsworth": "grafton",
    "Enfield": "grafton",
    "Franconia": "grafton",
    "Grafton": "grafton",
    "Groton": "grafton",
    "Hanover": "grafton",
    "Haverhill": "grafton",
    "Hebron": "grafton",
    "Holderness": "grafton",
    "Landaff": "grafton",
    "Lebanon": "grafton",
    "Lincoln": "grafton",
    "Lisbon": "grafton",
    "Littleton": "grafton",
    "Lyman": "grafton",
    "Lyme": "grafton",
    "Monroe": "grafton",
    "Orange": "grafton",
    "Orford": "grafton",
    "Piermont": "grafton",
    "Plymouth": "grafton",
    "Rumney": "grafton",
    "Sugar Hill": "grafton",
    "Thornton": "grafton",
    "Warren": "grafton",
    "Waterville Valley": "grafton",
    "Wentworth": "grafton",
    "Woodstock": "grafton",

    # Coos County (21 municipalities)
    "Berlin": "coos",
    "Carroll": "coos",
    "Clarksville": "coos",
    "Colebrook": "coos",
    "Columbia": "coos",
    "Dalton": "coos",
    "Dummer": "coos",
    "Errol": "coos",
    "Gorham": "coos",
    "Jefferson": "coos",
    "Lancaster": "coos",
    "Milan": "coos",
    "Millsfield": "coos",
    "Northumberland": "coos",
    "Pittsburg": "coos",
    "Randolph": "coos",
    "Shelburne": "coos",
    "Stark": "coos",
    "Stewartstown": "coos",
    "Stratford": "coos",
    "Whitefield": "coos",
}


def get_registry_for_nh_town(town_name: str) -> Optional[str]:
    """
    Get the registry county for a given New Hampshire municipality.

    Args:
        town_name: Municipality name (e.g., "Portsmouth", "Manchester")

    Returns:
        Registry county string (e.g., "rockingham") or None if not found

    Example:
        >>> get_registry_for_nh_town("Portsmouth")
        'rockingham'
        >>> get_registry_for_nh_town("Nashua")
        'hillsborough'
    """
    return NH_TOWN_TO_REGISTRY.get(town_name)


def get_all_nh_registries() -> list[str]:
    """
    Get a list of all unique NH registry counties.

    Returns:
        Sorted list of registry county names
    """
    return sorted(set(NH_TOWN_TO_REGISTRY.values()))


def get_nh_towns_for_registry(registry_county: str) -> list[str]:
    """
    Get all municipalities that belong to a given registry county.

    Args:
        registry_county: Registry county name (e.g., "rockingham")

    Returns:
        List of municipality names in that registry county
    """
    return [town for town, county in NH_TOWN_TO_REGISTRY.items() if county == registry_county]


def get_all_nh_towns() -> list[str]:
    """
    Get a sorted list of all NH municipalities.

    Returns:
        Sorted list of all 234 NH municipality names
    """
    return sorted(NH_TOWN_TO_REGISTRY.keys())
