use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use parquet::basic::Compression;
use parquet::file::{properties::WriterProperties, writer::SerializedFileWriter};
use reqwest::blocking::Client;
use scraper::{Html, Selector};

const MAX_ARTICLES: usize = 5000;
const SEED_URLS: [&str; 5] = [
    "https://en.wikipedia.org/wiki/Physics",
    "https://en.wikipedia.org/wiki/Astronomy",
    "https://en.wikipedia.org/wiki/Mathematics",
    "https://en.wikipedia.org/wiki/Biology",
    "https://en.wikipedia.org/wiki/Computer_science",
];

#[tokio::main]
async fn main() {
    let visited_urls = Arc::new(Mutex::new(HashSet::new()));
    let url_pool = Arc::new(Mutex::new(VecDeque::from(
        SEED_URLS.iter().map(|&s| s.to_string()),
    )));

    // create parquet file
    let mut parquet_writer = SerializedFileWriter::new(
        "stem_wikipedia.parquet",
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
        vec![
            parquet::file::metadata::ColumnDescriptor::new(
                "title",
                parquet::file::metadata::Type::BYTE_ARRAY,
                false,
            ),
            parquet::file::metadata::ColumnDescriptor::new(
                "paragraph",
                parquet::file::metadata::Type::BYTE_ARRAY,
                false,
            ),
        ],
    )?;

    let mut articles_scraped = 0;

    while articles_scraped < MAX_ARTICLES {
        let url = url_pool.pop_front().unwrap();
        let visited_urls = visited_urls.clone();
        let url_pool = url_pool.clone();
        let parquet_writer = parquet_writer.clone();

        tokio::spawn(async move {
            if !visited_urls.lock().unwrap().contains(&url) {
                visited_urls.lock().unwrap().insert(url.clone());
                match fetch_article(&url).await {
                    Ok(html) => {
                        process_article(&html, &mut parquet_writer).await;
                    }
                    Err(e) => {
                        eprintln!("Error fetching {}: {}", url, e);
                    }
                }
            }

            articles_scraped += 1;
        });
    }
}

async fn fetch_article(url: &str) -> Result<String, reqwest::Error> {
    let client = Client::new();
    let response = client.get(url).send().await?;
    response.text().await
}

async fn process_article(html: &str, output_file: &mut FileWriter) {
    // extract title, first paragraph, and links

    let document = Html::parse_document(html);
    let title_selector = Selector::parse("h1#firstHeading").unwrap();
    let paragraph_selector = Selector::parse("div.mw-parser-output > p").unwrap();
    let link_selector = Selector::parse("a").unwrap();

    if let Some(title_element) = document.select(&title_selector).next() {
        if let Some(paragraph_element) = document.select(&paragraph_selector).next() {
            let title = title_element.text().collect::<String>();
            let paragraph = paragraph_element.text().collect::<String>();

            // title on the first column, paragraph on the second column
            writeln!(csvfile, "{},{}", title, paragraph).unwrap();

            for link in document.select(&link_selector) {
                if let Some(href) = link.value().attr("href") {
                    if href.starts_with("/wiki/") {
                        let link = format!("https://en.wikipedia.org{}", href);
                        tx.send(link).await.unwrap();
                    }
                }
            }
        }
    }
}
