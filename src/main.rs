// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use futures::StreamExt;
use std::{path::PathBuf, str::FromStr, sync::Arc};
use sui_sdk::SuiClientBuilder;
use sui_types::{
    base_types::{ObjectID, SuiAddress}, transaction::{GasData, ObjectArg, Transaction, TransactionData, TransactionKind},
};
use sui_keys::keypair_file::read_key;
use sui_json_rpc_types::{SuiObjectDataOptions, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponseOptions};


#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
struct Args {
    #[clap(long)]
    pub key_path: PathBuf,

    #[clap(long)]
    pub rpc_url: String,

}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let args = Args::parse();
    let sui_client = Arc::new(SuiClientBuilder::default()
        .build(args.rpc_url.clone())
        .await?);
    let key = read_key(&args.key_path, false).unwrap();
    let address = SuiAddress::from(&key.public());
    assert_eq!(address, SuiAddress::from_str("0x8f7472504821715512572f29b521d10af0b11ecd27f887f6c7a55ad020c184e7").unwrap());
    let rgp = sui_client.governance_api().get_reference_gas_price().await.unwrap();
    let mut total_rebate = 0;
    let gas = ObjectID::from_hex_literal("0x0173200e109a96c232f34460becef9251a198b2c9bd08dfe1260ece6a90635eb").unwrap();
    let mut gas_obj = sui_client.read_api().get_object_with_options(gas, SuiObjectDataOptions::new()).await.unwrap().data.unwrap().object_ref();
    loop {
        let stream = sui_client.coin_read_api().get_coins_stream(address, Some("0x2::sui::SUI".to_string()));
        let coins = stream
            .take(512)
            .map(|coin| {
                (coin.balance, coin.object_ref())
            })
            .collect::<Vec<_>>()
            .await;
        let len1 = coins.len();
        let coins = coins.into_iter().filter(|c| c.1.0 != gas).collect::<Vec<_>>();
        let len2 = coins.len();
        let pt = {
            let mut builder =
                sui_types::programmable_transaction_builder::ProgrammableTransactionBuilder::new();
            let mut coin_args = coins
                .iter()
                .map(|(_, obj_ref)| builder.obj(ObjectArg::ImmOrOwnedObject(*obj_ref)).unwrap())
                .collect::<Vec<_>>();
            let first_arg = coin_args.remove(0);
            builder.command(sui_types::transaction::Command::MergeCoins(first_arg, coin_args));
            builder.finish()
        };

        let kind = TransactionKind::ProgrammableTransaction(pt);
        let gas_data = GasData {
            payment: vec![gas_obj],
            owner: address,
            price: rgp,
            budget: 5_000_000_000,
        };
        let tx_data = TransactionData::new_with_gas_data(kind, address, gas_data);
        let tx = Transaction::from_data_and_signer(tx_data, vec![&key]);
        let resp = sui_client.quorum_driver_api().execute_transaction_block(tx, SuiTransactionBlockResponseOptions::new().with_effects(), None).await.unwrap();
        assert_eq!(resp.status_ok(), Some(true));
        let effects = resp.effects.unwrap();
        let rebate = effects.gas_cost_summary().net_gas_usage();
        gas_obj = effects.gas_object().reference.to_object_ref();
        println!("merged {len2} coins and got back {} SUI", rebate / -1_000_000_000);
        total_rebate += rebate;
        if len1 < 512 {
            println!("Done.");
            break;
        }
    }
    println!("Got back {} Sui in total", total_rebate / -1_000_000_000);
    Ok(())
}
