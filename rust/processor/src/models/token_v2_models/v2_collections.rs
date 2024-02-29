// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// This is required because a diesel macro makes clippy sad
#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use super::v2_token_utils::{TokenStandard, V2TokenResource};
use crate::{
    models::{
        default_models::move_resources::MoveResource,
        object_models::v2_object_utils::ObjectAggregatedDataMapping,
        token_models::{
            collection_datas::{CollectionData, QUERY_RETRIES, QUERY_RETRY_DELAY_MS},
            token_utils::{CollectionDataIdType, TokenWriteSet},
            tokens::TableHandleToOwner,
        },
    },
    schema::{collections_v2, current_collections_v2},
    utils::{database::PgPoolConnection, util::standardize_address},
};
use anyhow::Context;
use aptos_protos::transaction::v1::{WriteResource, WriteTableItem};
use bigdecimal::{BigDecimal, Zero};
use diesel::{prelude::*, sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

// PK of current_collections_v2, i.e. collection_id
pub type CurrentCollectionV2PK = String;

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(transaction_version, write_set_change_index))]
#[diesel(table_name = collections_v2)]
pub struct CollectionV2 {
    pub transaction_version: i64,
    pub write_set_change_index: i64,
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub token_standard: String,
    pub transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(collection_id))]
#[diesel(table_name = current_collections_v2)]
pub struct CurrentCollectionV2 {
    pub collection_id: String,
    pub creator_address: String,
    pub collection_name: String,
    pub description: String,
    pub uri: String,
    pub current_supply: BigDecimal,
    pub max_supply: Option<BigDecimal>,
    pub total_minted_v2: Option<BigDecimal>,
    pub mutable_description: Option<bool>,
    pub mutable_uri: Option<bool>,
    pub table_handle_v1: Option<String>,
    pub token_standard: String,
    pub last_transaction_version: i64,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
}

#[derive(Debug, QueryableByName)]
pub struct CreatorFromCollectionTableV1 {
    #[diesel(sql_type = Text)]
    pub creator_address: String,
}

impl CollectionV2 {
    pub fn get_v2_from_write_resource(
        write_resource: &WriteResource,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        object_metadatas: &ObjectAggregatedDataMapping,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        let type_str = MoveResource::get_outer_type_from_resource(write_resource);
        if !V2TokenResource::is_resource_supported(type_str.as_str()) {
            return Ok(None);
        }
        let resource = MoveResource::from_write_resource(
            write_resource,
            0, // Placeholder, this isn't used anyway
            txn_version,
            0, // Placeholder, this isn't used anyway
        );

        if let V2TokenResource::Collection(inner) = &V2TokenResource::from_resource(
            &type_str,
            resource.data.as_ref().unwrap(),
            txn_version,
        )? {
            let (mut current_supply, mut max_supply, mut total_minted_v2) =
                (BigDecimal::zero(), None, None);
            let (mut mutable_description, mut mutable_uri) = (None, None);
            if let Some(object_data) = object_metadatas.get(&resource.address) {
                // Getting supply data (prefer fixed supply over unlimited supply although they should never appear at the same time anyway)
                let fixed_supply = object_data.fixed_supply.as_ref();
                let unlimited_supply = object_data.unlimited_supply.as_ref();
                if let Some(supply) = unlimited_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.clone(),
                        None,
                        Some(supply.total_minted.clone()),
                    );
                }
                if let Some(supply) = fixed_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.clone(),
                        Some(supply.max_supply.clone()),
                        Some(supply.total_minted.clone()),
                    );
                }

                // Aggregator V2 enables a separate struct for supply
                let concurrent_supply = object_data.concurrent_supply.as_ref();
                if let Some(supply) = concurrent_supply {
                    (current_supply, max_supply, total_minted_v2) = (
                        supply.current_supply.value.clone(),
                        if supply.current_supply.max_value == u64::MAX.into() {
                            None
                        } else {
                            Some(supply.current_supply.max_value.clone())
                        },
                        Some(supply.total_minted.value.clone()),
                    );
                }

                // Getting collection mutability config from AptosCollection
                let collection = object_data.aptos_collection.as_ref();
                if let Some(collection) = collection {
                    mutable_description = Some(collection.mutable_description);
                    mutable_uri = Some(collection.mutable_uri);
                }
            } else {
                // ObjectCore should not be missing, returning from entire function early
                return Ok(None);
            }

            let collection_id = resource.address.clone();
            let creator_address = inner.get_creator_address();
            let collection_name = inner.get_name_trunc();
            let description = inner.description.clone();
            let uri = inner.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: creator_address.clone(),
                    collection_name: collection_name.clone(),
                    description: description.clone(),
                    uri: uri.clone(),
                    current_supply: current_supply.clone(),
                    max_supply: max_supply.clone(),
                    total_minted_v2: total_minted_v2.clone(),
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                    transaction_timestamp: txn_timestamp,
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address,
                    collection_name,
                    description,
                    uri,
                    current_supply,
                    max_supply,
                    total_minted_v2,
                    mutable_description,
                    mutable_uri,
                    table_handle_v1: None,
                    token_standard: TokenStandard::V2.to_string(),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    pub async fn get_v1_from_write_table_item(
        table_item: &WriteTableItem,
        txn_version: i64,
        write_set_change_index: i64,
        txn_timestamp: chrono::NaiveDateTime,
        table_handle_to_owner: &TableHandleToOwner,
        // conn: &mut PgPoolConnection<'_>,
    ) -> anyhow::Result<Option<(Self, CurrentCollectionV2)>> {
        let table_item_data = table_item.data.as_ref().unwrap();

        let maybe_collection_data = match TokenWriteSet::from_table_item_type(
            table_item_data.value_type.as_str(),
            &table_item_data.value,
            txn_version,
        )? {
            Some(TokenWriteSet::CollectionData(inner)) => Some(inner),
            _ => None,
        };
        if let Some(collection_data) = maybe_collection_data {
            let table_handle = table_item.handle.to_string();
            let maybe_creator_address = table_handle_to_owner
                .get(&standardize_address(&table_handle))
                .map(|table_metadata| table_metadata.get_owner_address());
            let mut creator_address = match maybe_creator_address {
                Some(ca) => ca,
                None => return Ok(None),
                // None => {
                //     match Self::get_collection_creator_for_v1(conn, &table_handle)
                //         .await
                //         .context(format!(
                //             "Failed to get collection creator for table handle {}, txn version {}",
                //             table_handle, txn_version
                //         )) {
                //         Ok(ca) => ca,
                //         Err(_) => {
                //             // Try our best by getting from the older collection data
                //             match CollectionData::get_collection_creator(conn, &table_handle).await
                //             {
                //                 Ok(creator) => creator,
                //                 Err(_) => {
                //                     tracing::error!(
                //                         transaction_version = txn_version,
                //                         lookup_key = &table_handle,
                //                         "Failed to get collection v2 creator for table handle. You probably should backfill db."
                //                     );
                //                     return Ok(None);
                //                 },
                //             }
                //         },
                //     }
                // },
            };
            creator_address = standardize_address(&creator_address);
            let collection_id_struct =
                CollectionDataIdType::new(creator_address, collection_data.get_name().to_string());
            let collection_id = collection_id_struct.to_id();
            let collection_name = collection_data.get_name_trunc();
            let uri = collection_data.get_uri_trunc();

            Ok(Some((
                Self {
                    transaction_version: txn_version,
                    write_set_change_index,
                    collection_id: collection_id.clone(),
                    creator_address: collection_id_struct.creator.clone(),
                    collection_name: collection_name.clone(),
                    description: collection_data.description.clone(),
                    uri: uri.clone(),
                    current_supply: collection_data.supply.clone(),
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle.clone()),
                    token_standard: TokenStandard::V1.to_string(),
                    transaction_timestamp: txn_timestamp,
                },
                CurrentCollectionV2 {
                    collection_id,
                    creator_address: collection_id_struct.creator,
                    collection_name,
                    description: collection_data.description,
                    uri,
                    current_supply: collection_data.supply,
                    max_supply: Some(collection_data.maximum.clone()),
                    total_minted_v2: None,
                    mutable_uri: Some(collection_data.mutability_config.uri),
                    mutable_description: Some(collection_data.mutability_config.description),
                    table_handle_v1: Some(table_handle),
                    token_standard: TokenStandard::V1.to_string(),
                    last_transaction_version: txn_version,
                    last_transaction_timestamp: txn_timestamp,
                },
            )))
        } else {
            Ok(None)
        }
    }

    // async fn get_collection_creator_for_v1(
    //     conn: &mut PgPoolConnection<'_>,
    //     table_handle: &str,
    // ) -> anyhow::Result<String> {
    //     let mut retried = 0;
    //     while retried < QUERY_RETRIES {
    //         retried += 1;
    //         match Self::get_by_table_handle(conn, table_handle).await {
    //             Ok(creator) => return Ok(creator),
    //             Err(_) => {
    //                 tokio::time::sleep(std::time::Duration::from_millis(QUERY_RETRY_DELAY_MS))
    //                     .await;
    //             },
    //         }
    //     }
    //     Err(anyhow::anyhow!("Failed to get collection creator"))
    // }

    // async fn get_by_table_handle(
    //     conn: &mut PgPoolConnection<'_>,
    //     table_handle: &str,
    // ) -> anyhow::Result<String> {
    //     let mut res: Vec<Option<CreatorFromCollectionTableV1>> = sql_query(
    //         "SELECT creator_address FROM current_collections_v2 WHERE table_handle_v1 = $1",
    //     )
    //     .bind::<Text, _>(table_handle)
    //     .get_results(conn)
    //     .await?;
    //     Ok(res
    //         .pop()
    //         .context("collection result empty")?
    //         .context("collection result null")?
    //         .creator_address)
    // }
}
