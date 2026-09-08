//! Provider-neutral component identities for the V1 Macro feature vector.

const MACRO_RATE_UNIT: &str = "percent_per_year";
const MACRO_LABOR_UNIT: &str = "percent_of_labor_force";

/// One exact provider-neutral Macro component admitted by the V1 feature product.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FeatureDatasetMacroComponentDescriptor {
    position: u8,
    indicator_id: &'static str,
    component_name: &'static str,
    unit: &'static str,
}

impl FeatureDatasetMacroComponentDescriptor {
    const fn new(
        position: u8,
        indicator_id: &'static str,
        component_name: &'static str,
        unit: &'static str,
    ) -> Self {
        Self {
            position,
            indicator_id,
            component_name,
            unit,
        }
    }

    /// Returns the zero-based economic vector position.
    pub const fn position(self) -> u8 {
        self.position
    }

    /// Returns the stable provider-neutral Macro-context indicator identity.
    pub const fn indicator_id(self) -> &'static str {
        self.indicator_id
    }

    /// Returns the stable feature component identity persisted in immutable datasets.
    pub const fn component_name(self) -> &'static str {
        self.component_name
    }

    /// Returns the provider-neutral canonical analytical unit.
    pub const fn unit(self) -> &'static str {
        self.unit
    }
}

const FEATURE_DATASET_MACRO_COMPONENTS_V1: [FeatureDatasetMacroComponentDescriptor; 12] = [
    FeatureDatasetMacroComponentDescriptor::new(
        0,
        "us-government-yield-1m",
        "research.macro.us-government-yield-1m",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        1,
        "us-government-yield-3m",
        "research.macro.us-government-yield-3m",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        2,
        "us-government-yield-6m",
        "research.macro.us-government-yield-6m",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        3,
        "us-government-yield-1y",
        "research.macro.us-government-yield-1y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        4,
        "us-government-yield-2y",
        "research.macro.us-government-yield-2y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        5,
        "us-government-yield-3y",
        "research.macro.us-government-yield-3y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        6,
        "us-government-yield-5y",
        "research.macro.us-government-yield-5y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        7,
        "us-government-yield-7y",
        "research.macro.us-government-yield-7y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        8,
        "us-government-yield-10y",
        "research.macro.us-government-yield-10y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        9,
        "us-government-yield-20y",
        "research.macro.us-government-yield-20y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        10,
        "us-government-yield-30y",
        "research.macro.us-government-yield-30y",
        MACRO_RATE_UNIT,
    ),
    FeatureDatasetMacroComponentDescriptor::new(
        11,
        "us-unemployment-rate",
        "research.macro.us-unemployment-rate",
        MACRO_LABOR_UNIT,
    ),
];

/// Returns the single code-owned V1 Macro component registry in economic curve order.
pub const fn feature_dataset_macro_components_v1()
-> &'static [FeatureDatasetMacroComponentDescriptor; 12] {
    &FEATURE_DATASET_MACRO_COMPONENTS_V1
}
