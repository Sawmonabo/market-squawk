use std::collections::BTreeMap;
use std::error::Error;

use rust_decimal::Decimal;
use serde_json::json;

use crate::{
    BeaCompleteness, BeaDatasetIdentity, BeaError, BeaMetadataGeneration, BeaMetadataRecords,
    BeaMissingValue, BeaObservationValue, BeaParameterIdentity, BeaQuery, BeaUserId,
    parse_data_page, parse_metadata_page,
};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";

#[test]
fn metadata_and_data_contract_preserves_exact_dimensions_values_notes_and_counts()
-> Result<(), Box<dyn Error>> {
    let user_id = BeaUserId::try_new(USER_ID.to_owned())?;
    let dataset = BeaDatasetIdentity::try_new("Regional")?;
    let metadata_request = BeaQuery::parameter_list(dataset.clone())?.single_page(Some(2))?;
    let metadata_bytes = serde_json::to_vec(&json!({
        "BEAAPI": {
            "Request": {"RequestParam": [
                {"ParameterName": "USERID", "ParameterValue": USER_ID},
                {"ParameterName": "METHOD", "ParameterValue": "GETPARAMETERLIST"},
                {"ParameterName": "DATASETNAME", "ParameterValue": "REGIONAL"},
                {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
            ]},
            "Results": {"Parameter": [
                {
                    "ParameterName": "TableName",
                    "ParameterDataType": "string",
                    "ParameterDescription": "Regional table",
                    "ParameterIsRequiredFlag": "1",
                    "MultipleAcceptedFlag": "0"
                },
                {
                    "ParameterName": "Year",
                    "ParameterDataType": "string",
                    "ParameterDescription": "Requested years",
                    "ParameterIsRequiredFlag": "0",
                    "ParameterDefaultValue": "LAST5",
                    "MultipleAcceptedFlag": "1",
                    "AllValue": "ALL"
                }
            ]}
        }
    }))?;
    let metadata = parse_metadata_page(
        &metadata_bytes,
        &metadata_request,
        &user_id,
        crate::BeaParseLimits::production_defaults(),
    )?;
    let BeaMetadataRecords::Parameters(parameters) = metadata.records() else {
        return Err(Box::new(BeaError::InvalidField("parameter metadata")));
    };
    assert_eq!(parameters.len(), 2);
    assert!(parameters[0].is_required());
    assert_eq!(parameters[1].all_value(), Some("ALL"));
    assert_eq!(metadata.receipt().completeness(), BeaCompleteness::Complete);

    let generation =
        BeaMetadataGeneration::from_response_digests(&[metadata.receipt().response_digest()])?;
    let data_request = data_request(dataset, generation, Some(2))?;
    let authorized = data_request.authorize(&user_id)?;
    assert!(authorized.expose_url().contains("UserID=11111111-2222"));
    assert!(!format!("{authorized:?}").contains("11111111-2222"));

    let response = data_response("45,359", "", "2026-03-25T19:25:39.113")?;
    let data = parse_data_page(
        &response,
        &data_request,
        &user_id,
        crate::BeaParseLimits::production_defaults(),
    )?;
    assert_eq!(data.observations().len(), 2);
    assert_eq!(data.observations()[0].identity().table(), Some("SAINC1"));
    assert_eq!(data.observations()[0].identity().line(), Some("CAINC1-3"));
    assert_eq!(
        data.observations()[0].value().observed(),
        Some("45359".parse::<Decimal>()?)
    );
    assert_eq!(data.observations()[0].unit().cl_unit(), "Dollars");
    assert_eq!(data.observations()[0].unit().unit_multiplier(), 0);
    assert_eq!(data.observations()[0].note_references(), ["2", "*"]);
    assert_eq!(
        data.observations()[1].value(),
        &BeaObservationValue::Missing(BeaMissingValue::Blank)
    );
    assert_eq!(data.receipt().returned_rows(), 2);
    assert_eq!(data.receipt().missing_rows(), Some(0));
    assert_eq!(data.receipt().completeness(), BeaCompleteness::Complete);
    Ok(())
}

fn data_request(
    dataset: BeaDatasetIdentity,
    generation: BeaMetadataGeneration,
    expected_rows: Option<usize>,
) -> Result<crate::BeaRequest, BeaError> {
    let mut parameters = BTreeMap::new();
    parameters.insert(BeaParameterIdentity::try_new("GeoFips")?, "DE".to_owned());
    parameters.insert(BeaParameterIdentity::try_new("LineCode")?, "3".to_owned());
    parameters.insert(
        BeaParameterIdentity::try_new("TableName")?,
        "SAINC1".to_owned(),
    );
    parameters.insert(BeaParameterIdentity::try_new("Year")?, "2014".to_owned());
    BeaQuery::data(dataset, parameters, generation)?.single_page(expected_rows)
}

fn data_response(
    first_value: &str,
    second_value: &str,
    production_time: &str,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&json!({
        "BEAAPI": {
            "Request": {"RequestParam": [
                {"ParameterName": "USERID", "ParameterValue": USER_ID},
                {"ParameterName": "METHOD", "ParameterValue": "GETDATA"},
                {"ParameterName": "DATASETNAME", "ParameterValue": "REGIONAL"},
                {"ParameterName": "GEOFIPS", "ParameterValue": "DE"},
                {"ParameterName": "LINECODE", "ParameterValue": "3"},
                {"ParameterName": "TABLENAME", "ParameterValue": "SAINC1"},
                {"ParameterName": "YEAR", "ParameterValue": "2014"},
                {"ParameterName": "RESULTFORMAT", "ParameterValue": "JSON"}
            ]},
            "Results": {
                "Statistic": "Per capita personal income",
                "UnitOfMeasure": "Dollars",
                "PublicTable": "SAINC1 State annual personal income summary",
                "UTCProductionTime": production_time,
                "NoteRef": "2",
                "Dimensions": [
                    {"Ordinal": "1", "Name": "Code", "DataType": "string", "IsValue": "0"},
                    {"Ordinal": "2", "Name": "GeoFips", "DataType": "string", "IsValue": "0"},
                    {"Ordinal": "3", "Name": "GeoName", "DataType": "string", "IsValue": "0"},
                    {"Ordinal": "4", "Name": "TimePeriod", "DataType": "string", "IsValue": "0"},
                    {"Ordinal": "5", "Name": "DataValue", "DataType": "numeric", "IsValue": "1"},
                    {"Ordinal": "6", "Name": "CL_UNIT", "DataType": "string", "IsValue": "0"},
                    {"Ordinal": "7", "Name": "UNIT_MULT", "DataType": "numeric", "IsValue": "0"}
                ],
                "Data": [
                    {
                        "Code": "CAINC1-3", "GeoFips": "10000", "GeoName": "Delaware",
                        "TimePeriod": "2014", "CL_UNIT": "Dollars", "UNIT_MULT": "0",
                        "DataValue": first_value, "NoteRef": "2 *"
                    },
                    {
                        "Code": "CAINC1-3", "GeoFips": "10001", "GeoName": "Kent",
                        "TimePeriod": "2014", "CL_UNIT": "Dollars", "UNIT_MULT": "0",
                        "DataValue": second_value, "NoteRef": "2"
                    }
                ],
                "Notes": [
                    {"NoteRef": "2", "NoteText": "Per capita personal income."},
                    {"NoteRef": "*", "NoteText": "Source-specific note."},
                    {"NoteRef": "", "NoteText": "Release revised prior years."}
                ]
            }
        }
    }))
}
