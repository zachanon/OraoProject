/*
We are receiving entries of data from different data providers
Every data provider tries it’s best to send data with a certain frequency or when a an update event has occurred on the data source side.

For example, a provider can state that it is sending data every second. That equates to 3600 data updates during a duration of one hour.
Please take into account the following situations:

Data might not arrive at exactly 1 second, it may be offset by +- 100 milliseconds. negative score if data is the same, positive score if data is outside stddev
The provider might stop sending data for a certain interval. negative score.
The provider might send the same value several times in a row. negative score

Output:
Read min/max/stddev of numerical values. Keep in mind, data value might be a char, not numerical.
Delete duplicates. If we have the same values within a timestamp (offset of +-100 ms) it means that we have a duplicate.
 If we have a small difference of the same numerical value (use min/max/stddev from #1) value within a timestamp (offset of +- 100ms) it means we have a duplicate.
Calculate the average gap If we have at least one bad line, we have a gap of 1,
 if we have 10 bad lines sequencing, we have a gap of 10. We need to calculate the maximum gaps per each data provider
Output the new list and total gaps per provider

Bonus points:
Average - calculate and output average values for each grouped timestamp
Average 2 - calculate and output average of values with highest score (take situations mentioned above and apply as weight/score) inside a timestamp group
HTTP request the data directly from the server, instead of using a local file (hint: Hyper, and HyperTLS).


Integration:
standard cargo run that reads the sample data and produces output

Sample data:
https://raw.githubusercontent.com/orao-network/sample-data/main/data.json
*/
#![feature(iter_zip)]

use std::collections::HashMap;
use std::error::Error;
use std::iter::zip;

use serde::Deserialize;

#[derive(Debug)]
struct DataItem {
    value: Vec<f32>,
    timestamp: Vec<u64>,
}
#[derive(Debug)]
struct Statistics {
    min: f32,
    max: f32,
    std: f32,
}

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let raw_json =
        reqwest::get("https://raw.githubusercontent.com/orao-network/sample-data/main/data.json")
            .await?
            .text()
            .await?;

    let data = parse_data(&raw_json).unwrap();

    let data_statistics = parse_data_statistics(&data).unwrap();

    let (cleaned_data, gap_statistics) = clean_data(&data, &data_statistics);

    println!("{:#?}", gap_statistics);
    Ok(())
}
fn parse_data(raw_input: &String) -> Result<HashMap<u32, HashMap<u32, DataItem>>, Box<dyn Error>> {
    #[derive(Deserialize)]
    struct RawDataItem {
        provider_id: u32,
        key: u32,
        value: serde_json::Value,
        timestamp: u64,
    }

    let raw_data: Vec<RawDataItem> = serde_json::from_str(raw_input)?;

    let mut provider_data = HashMap::<u32, HashMap<u32, DataItem>>::new();
    for data_item in raw_data {
        if !provider_data.contains_key(&data_item.provider_id) {
            provider_data.insert(data_item.provider_id, HashMap::<u32, DataItem>::new());
        }

        let data = provider_data.get_mut(&data_item.provider_id).unwrap();
        if !data.contains_key(&data_item.key) {
            data.insert(
                data_item.key,
                DataItem {
                    value: Vec::new(),
                    timestamp: Vec::new(),
                },
            );
        }

        // Confirm data is numerical, pass if categorical
        if !data_item.value.is_number() {
            continue;
        }
        data.get_mut(&data_item.key)
            .unwrap()
            .value
            .push(data_item.value.as_f64().unwrap() as f32);
        data.get_mut(&data_item.key)
            .unwrap()
            .timestamp
            .push(data_item.timestamp);
    }
    Ok(provider_data)
}

fn parse_data_statistics(
    data: &HashMap<u32, HashMap<u32, DataItem>>,
) -> Result<HashMap<u32, HashMap<u32, Statistics>>, Box<dyn Error>> {
    let mut statistics = HashMap::<u32, HashMap<u32, Statistics>>::new();

    for provider in data.keys() {
        statistics.insert(*provider, HashMap::<u32, Statistics>::new());
        for data_id in data.get(provider).unwrap().keys() {
            let value = &data.get(provider).unwrap().get(data_id).unwrap().value;
            statistics.get_mut(provider).unwrap().insert(
                *data_id,
                Statistics {
                    min: value.iter().fold(f32::MAX, |a, &b| a.min(b)),
                    max: value.iter().fold(f32::MIN, |a, &b| a.max(b)),
                    std: {
                        let mean: f32 = value.iter().sum::<f32>() / value.len() as f32;
                        let variance = value
                            .iter()
                            .map(|item| {
                                let diff = mean - (*item as f32);

                                diff * diff
                            })
                            .sum::<f32>()
                            / value.len() as f32;

                        variance.sqrt()
                    },
                },
            );
        }
    }
    Ok(statistics)
}

fn clean_data(
    data: &HashMap<u32, HashMap<u32, DataItem>>,
    data_statistics: &HashMap<u32, HashMap<u32, Statistics>>,
) -> (
    HashMap<u32, HashMap<u32, DataItem>>,
    HashMap<u32, (u32, u32)>,
) {
    let mut cleaned_data = HashMap::<u32, HashMap<u32, DataItem>>::new();
    let mut gap_statistics = HashMap::<u32, (u32, u32)>::new();

    for provider in data.keys() {
        cleaned_data.insert(*provider, HashMap::<u32, DataItem>::new());
        gap_statistics.insert(*provider, (0, 0));

        for data_id in data.get(provider).unwrap().keys() {
            cleaned_data.get_mut(provider).unwrap().insert(
                *data_id,
                DataItem {
                    value: Vec::new(),
                    timestamp: Vec::new(),
                },
            );
            let data_item = data.get(provider).unwrap().get(data_id).unwrap();
            let item_std = data_statistics
                .get(provider)
                .unwrap()
                .get(data_id)
                .unwrap()
                .std;

            let mut prevtime: u64 = 0;
            let mut prevval: f32 = f32::NAN;
            let mut maxgap: u32 = 0;
            let mut gapcount: u32 = 0;
            for (value, time) in zip(&data_item.value, &data_item.timestamp) {
                if prevtime == 0 {
                    prevtime = *time;
                    prevval = *value;
                    continue;
                }

                let diff = match time.checked_sub(prevtime) {
                    Some(diff) => diff,
                    _ => 0,
                };
                // Wasn't sure about the cutoff for significant difference. Decided on half a std for this implementation
                if (diff < 100) && (value - prevval).abs() < (item_std / 2.0) {
                    let (mut total_gaps, mut max_count) = gap_statistics.get(provider).unwrap();
                    total_gaps += 1;
                    gapcount += 1;
                    if gapcount > maxgap {
                        max_count = gapcount;
                        maxgap = gapcount;
                    }
                    gap_statistics.insert(*provider, (total_gaps, max_count));
                    continue;
                }

                cleaned_data
                    .get_mut(provider)
                    .unwrap()
                    .get_mut(data_id)
                    .unwrap()
                    .value
                    .push(*value);
                cleaned_data
                    .get_mut(provider)
                    .unwrap()
                    .get_mut(data_id)
                    .unwrap()
                    .timestamp
                    .push(*time);

                prevtime = *time;
                prevval = *value;
                gapcount = 0;
            }
        }
    }

    (cleaned_data, gap_statistics)
}
