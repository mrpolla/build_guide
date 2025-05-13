import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

# FULL ENRICHED SET OF INDICATORS (shortened for brevity in this snippet)
indicators = {
    "PERE": (
        "Renewable primary energy (energy carrier)",
        "Measures renewable energy used as fuel during production.",
        "Quantifies the amount of renewable primary energy used specifically as an energy carrier. This includes sources like biomass, wind, solar, and geothermal energy consumed as fuel during production or use. Measured in megajoules (MJ), it does not include renewable energy used as raw materials, which is instead counted under PERM."
    ),
    "PERM": (
        "Renewable primary energy (material use)",
        "Tracks renewable input used as a material instead of fuel.",
        "Measures the use of renewable energy resources when applied as material feedstock rather than as energy carriers. For example, timber or plant-based polymers incorporated into the product. Reported in megajoules (MJ)."
    ),
    "PERT": (
        "Total renewable primary energy",
        "Total of renewable energy used for fuel and materials.",
        "This is the sum of PERE and PERM, capturing the total renewable energy footprint of a product. It reflects the full renewable energy demand in megajoules (MJ) across its life cycle."
    ),
    "PENRE": (
        "Non-renewable primary energy (energy carrier)",
        "Fossil energy used as fuel during product life cycle.",
        "Quantifies fossil energy resources used directly for energy generation during the life cycle. Common sources include coal, oil, and gas. Measured in megajoules (MJ)."
    ),
    "PENRM": (
        "Non-renewable primary energy (material use)",
        "Fossil energy used as raw material (e.g., plastics).",
        "Represents fossil-based materials that are not burned as fuel but are integrated as material components. Often includes synthetic polymers. Measured in megajoules (MJ)."
    ),
    "PENRT": (
        "Total non-renewable primary energy",
        "Combined use of fossil fuels as energy and material.",
        "Total fossil energy footprint, summing both energy use (PENRE) and material input (PENRM). Indicates total dependency on non-renewable resources. Measured in megajoules (MJ)."
    ),
    "SM": (
        "Secondary material use",
        "Tracks recycled material input to reduce demand for virgin raw resources.",
        "This indicator quantifies the amount of secondary (recycled) materials used as input into the production process. It reflects circular economy integration and reduces raw material dependency. Measured in kilograms (kg)."
    ),
    "RSF": (
        "Renewable secondary fuels",
        "Renewable fuels recovered from waste streams.",
        "Refers to energy carriers derived from renewable waste such as biogas or biodiesel. Contributes to renewable energy use beyond direct sources. Measured in megajoules (MJ)."
    ),
    "NRSF": (
        "Non-renewable secondary fuels",
        "Fossil-based waste fuels used for energy recovery.",
        "Includes materials like plastic or synthetic rubber incinerated for energy recovery. May reduce primary fuel demand but still contributes to fossil energy footprint. Measured in megajoules (MJ)."
    ),
    "FW": (
        "Net use of fresh water",
        "Quantifies water withdrawn and not returned to the original source in same quality.",
        "Tracks water consumption where water is removed from its source and not returned in the same quality or quantity. Reflects water stress and scarcity impacts. Measured in cubic meters (m³)."
    ),
    "HWD": (
        "Hazardous waste disposed",
        "Volume of toxic waste sent to landfills or incineration.",
        "Includes substances that are chemically reactive, toxic, corrosive, or otherwise harmful to humans and ecosystems. Measured in kilograms (kg)."
    ),
    "NHWD": (
        "Non-hazardous waste disposed",
        "Non-toxic waste discarded after processing or use.",
        "Covers municipal or industrial waste that does not pose chemical danger but is not recycled. Includes packaging, rubble, or production scrap. Measured in kilograms (kg)."
    ),
    "RWD": (
        "Radioactive waste disposed",
        "Radioactive waste generated and disposed over life cycle.",
        "Relevant for processes that include nuclear energy or material refining. Measured in kilograms (kg)."
    ),
    "CRU": (
        "Components for re-use",
        "Recovered components designed for direct reuse.",
        "Measures materials disassembled or removed for future reuse without reprocessing. Important for circular material cycles. Measured in kilograms (kg)."
    ),
    "MFR": (
        "Materials for recycling",
        "Materials collected and processed for recycling.",
        "Covers outputs separated for closed- or open-loop recycling. Part of end-of-life scenario modeling. Measured in kilograms (kg)."
    ),
    "MER": (
        "Materials for energy recovery",
        "Non-recyclable material recovered via incineration or other energy routes.",
        "Quantifies the portion of end-of-life waste treated to generate electricity or heat. Measured in kilograms (kg)."
    ),
    "EEE": (
        "Exported electrical energy",
        "Electricity produced and exported beyond system boundary.",
        "Typically arises from incineration with energy recovery or on-site solar systems. May count toward system credits. Measured in megajoules (MJ) or kilowatt-hours (kWh)."
    ),
    "EET": (
        "Exported thermal energy",
        "Heat energy produced and exported outside product boundary.",
        "Derived from waste incineration or cogeneration units. Contributes to system expansion credits. Measured in megajoules (MJ)."
    ),
    "GWP-fossil": (
        "Global Warming Potential (fossil)",
        "CO₂-equivalent emissions from fossil-based sources.",
        "Quantifies the greenhouse gas emissions from fossil fuel combustion or extraction. Most closely aligns with historical carbon accounting. Measured in kg CO₂-eq."
    ),
    "GWP-biogenic": (
        "Global Warming Potential (biogenic)",
        "CO₂-equivalent emissions and removals from biomass.",
        "Accounts for carbon cycles related to forest products or agricultural biomass. Includes emissions from decay or burning, and sequestration. Measured in kg CO₂-eq."
    ),
    "GWP-luluc": (
        "Global Warming Potential (land use and land use change)",
        "CO₂-equivalent emissions from changes in land cover.",
        "Covers carbon released or absorbed due to deforestation, afforestation, or crop conversion. Important in forestry and agriculture-linked LCAs."
    ),
    "ODP": (
        "Ozone depletion potential",
        "Potential for substances to destroy ozone in the stratosphere.",
        "Expressed in CFC-11 equivalents, this reflects the environmental hazard from refrigerants, solvents, or propellants. Measured in kg CFC-11-eq."
    ),
    "POCP": (
        "Photochemical ozone creation potential",
        "Ground-level ozone formation or smog potential.",
        "Often driven by VOCs and NOₓ emissions in sunlight. Measured in kg NMVOC-eq (non-methane volatile organic compounds)."
    ),
    "AP": (
        "Acidification potential",
        "Contribution to acid rain formation via SO₂, NOₓ, NH₃.",
        "Measured in kg SO₂-equivalent. Reflects emissions that acidify soil, freshwater, or affect vegetation."
    ),
    "EP-terrestrial": (
        "Eutrophication potential – terrestrial",
        "Soil nutrient saturation potential causing biodiversity loss.",
        "Primarily reflects nitrogen-based emissions that overstimulate plant growth in natural terrestrial systems. Measured in mol N-eq."
    ),
    "EP-freshwater": (
        "Eutrophication potential – freshwater",
        "Algal bloom risk in freshwater due to excess phosphorus.",
        "Driven mainly by phosphate emissions, often from wastewater or fertilizers. Measured in kg P-eq."
    ),
    "EP-marine": (
        "Eutrophication potential – marine",
        "Nutrient enrichment in oceans and coastal ecosystems.",
        "Reflects nitrogen and phosphorus leakage into saltwater environments. Measured in kg N-eq."
    ),
    "WDP": (
        "Water deprivation potential",
        "Water use weighted by scarcity in specific regions.",
        "Helps evaluate potential social and ecological stress from freshwater withdrawal. Measured in m³ world-eq deprived."
    ),
    "ADPE": (
        "Abiotic depletion potential – elements",
        "Scarcity-driven depletion of mineral and metal resources.",
        "Focuses on non-renewable, non-fossil resources such as copper, antimony, etc. Expressed in kg Sb-eq."
    ),
    "ADPF": (
        "Abiotic depletion potential – fossil fuels",
        "Depletion of fossil energy carriers like oil, gas, coal.",
        "Closely linked to PENRT but framed from a resource availability perspective. Measured in MJ."
    ),
    "HTP-c": (
        "Human toxicity potential – cancer effects",
        "Toxic impacts on human health due to carcinogens.",
        "Based on cancer-causing substances released to air, water, or soil. Expressed in comparative toxic units (CTUh)."
    ),
    "HTP-nc": (
        "Human toxicity potential – non-cancer effects",
        "Toxic impacts from substances with non-cancer effects.",
        "Covers other toxicological endpoints such as organ damage or developmental disorders. Measured in CTUh."
    ),
    "PM": (
        "Particulate matter formation",
        "Inhalable fine particles affecting respiratory health.",
        "PM10 and PM2.5 emitted directly or formed via precursors like NOₓ or SO₂. Reported in kg PM2.5-eq."
    ),
    "IR": (
        "Ionizing radiation",
        "Exposure to radioactive substances causing health risk.",
        "Measured in kBq U235-eq. Includes emissions from nuclear fuel cycles and certain industrial processes."
    ),
    "ETP-fw": (
        "Freshwater ecotoxicity potential",
        "Toxic stress on aquatic organisms from pollutants.",
        "Focuses on persistent, bioaccumulative, or acutely toxic substances. Measured in CTUe (comparative toxic units for ecosystems)."
    ),
    "SQP": (
        "Soil quality potential",
        "Potential degradation of soil functionality and ecosystem services.",
        "Captures effects from acidification, contamination, compaction, or erosion. Still under methodological development."
    ),
    "IRP": (
        "International Resource Panel (IRP) index",
        "Composite index of resource pressures from IRP methodology.",
        "Developed by UNEP to track global material flows and resource efficiency. May include multiple sub-indicators."
    ),
    "GWP": (
        "Global Warming Potential (unspecified)",
        "Ambiguous GWP value – validate source before use.",
        "Could represent total GWP or only fossil CO₂-eq. Legacy datasets may lack detail. Use GWP-total if clarity is ensured."
    ),
    "EP": (
        "Eutrophication Potential (unspecified)",
        "Unspecified eutrophication type (verify if marine/freshwater).",
        "Used as a generic placeholder when disaggregated EP values are not available."
    ),
    "SF": (
        "Stock flow resources",
        "Depletion of stock-flow categorized natural resources.",
        "Covers water, soil, and some renewable resource types managed over time. Conceptually distinct from fossil or mineral depletion."
    )
}

modules = {
    "A1": (
        "Raw material supply",
        "Covers raw material extraction and processing before manufacturing.",
        "Includes extraction and processing of primary and secondary materials, including fuels and energy carriers needed to produce construction products. Typically modeled using background datasets."
    ),
    "A2": (
        "Transport to manufacturer",
        "Accounts for material transport to the manufacturing facility.",
        "Covers all transport processes between the raw material supplier and the manufacturer. Includes fuel use, emissions, and transport-related infrastructure."
    ),
    "A3": (
        "Manufacturing",
        "Represents product manufacturing processes and packaging.",
        "Includes actual production processes at the factory, energy and material consumption, emissions, waste treatment, and packaging. Ends when the product leaves the gate."
    ),
    "A1-A3": (
        "Product stage (A1-A3)",
        "Summarizes raw material supply, transport, and manufacturing.",
        "Aggregate of stages A1 through A3, representing the total embodied environmental impact of the product before use. Commonly reported in cradle-to-gate EPDs."
    ),
    "B1": (
        "Use phase",
        "Reflects emissions or other impacts during the use of the product.",
        "Captures direct environmental impacts resulting from use, such as emissions during operation. May be zero for passive materials."
    ),
    "B2": (
        "Maintenance",
        "Tracks regular upkeep of the product over its lifetime.",
        "Includes water, energy, and material use needed to maintain the functionality of the product, including cleaning or part replacements."
    ),
    "B3": (
        "Repair",
        "Quantifies impacts from repairing the product during its lifetime.",
        "Includes labor, materials, and energy required for small-scale fixes or functional repairs without full replacement."
    ),
    "B4": (
        "Replacement",
        "Covers impacts of replacing the product or components.",
        "Accounts for removal and production of replacement parts or full products during the reference service life."
    ),
    "B5": (
        "Refurbishment",
        "Represents major updates to extend product life.",
        "Includes partial reconstruction or improvements to extend service life or performance beyond initial expectations."
    ),
    "B6": (
        "Operational energy use",
        "Measures energy consumed during the use phase.",
        "Includes electricity, heating, or cooling energy required for operating the product as intended."
    ),
    "B7": (
        "Operational water use",
        "Captures water consumed during normal operation.",
        "Quantifies all water directly consumed by the product in its operational phase."
    ),
    "C1": (
        "Deconstruction",
        "Deals with product removal or demolition.",
        "Includes manual or mechanical efforts to disassemble or demolish the product at end-of-life."
    ),
    "C2": (
        "Transport of waste",
        "Models logistics from site to treatment or disposal.",
        "Includes transport of deconstructed material to landfills, incinerators, or recycling centers."
    ),
    "C3": (
        "Waste processing",
        "Prepares waste for recycling or energy recovery.",
        "Includes sorting, shredding, cleaning, or preparation needed before final disposal or reuse."
    ),
    "C4": (
        "Disposal",
        "Covers landfill or incineration of unrecycled material.",
        "Represents final waste treatment with or without energy recovery for non-recyclable materials."
    ),
    "D": (
        "Beyond system boundary (reuse/recycle)",
        "Accounts for credits from reuse, recycling, or energy recovery.",
        "Models potential environmental benefits from avoided production due to reused/recycled outputs or exported energy. Applies only beyond the system boundary (e.g. credits for electricity sent to the grid)."
    )
}

def populate_indicators_and_modules():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for key, (name, short, long) in indicators.items():
        cur.execute("""
            INSERT INTO Indicators (indicator_key, name, short_description, long_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (indicator_key) DO UPDATE SET
                name = EXCLUDED.name,
                short_description = EXCLUDED.short_description,
                long_description = EXCLUDED.long_description
        """, (key, name, short, long))

    for code, (name, short, long) in modules.items():
        cur.execute("""
            INSERT INTO Modules (module_code, name, short_description, long_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (module_code) DO UPDATE SET
                name = EXCLUDED.name,
                short_description = EXCLUDED.short_description,
                long_description = EXCLUDED.long_description
        """, (code, name, short, long))

    conn.commit()
    cur.close()
    conn.close()
    print("Indicators and modules populated with enriched descriptions.")

if __name__ == "__main__":
    populate_indicators_and_modules()
