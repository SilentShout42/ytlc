mod parser;
mod search;

use clap::{Parser, Subcommand};
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub db_path: String,
}

impl DbConfig {
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }

    pub fn connect_path(&self) -> anyhow::Result<std::path::PathBuf> {
        let path = std::path::PathBuf::from(&self.db_path);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        Ok(path.canonicalize().unwrap_or(path))
    }
}

#[derive(Parser)]
#[command(about = "Process YouTube live chat data. Parse JSON files or search messages.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Search messages and print results as a markdown table.")]
    Search {
        #[arg(required = true, num_args = 1..)]
        regex_patterns: Vec<String>,

        #[arg(short, long, help = "File to write search results to.")]
        output_file: Option<String>,

        #[arg(long, help = "Include Author and Message columns in the output.")]
        debug: bool,
    },

    #[command(about = "Parse JSON files and load into SQLite.")]
    Parse {
        #[arg(help = "Directory containing .info.json and .live_chat.json files.")]
        data_dir: String,
    },

    #[command(about = "Test the database connection and show basic stats.")]
    Dbcheck,
}

fn main() -> anyhow::Result<()> {
    let db_path = std::env::var("YTLC_DB").unwrap_or_else(|_| "ytlc.db".to_string());

    let db_config = DbConfig::new(db_path);

    let cli = Cli::parse();

    match cli.command {
        Commands::Search {
            regex_patterns,
            output_file,
            debug,
        } => {
            search::print_search_results(
                &db_config,
                &regex_patterns,
                60,
                5,
                -10,
                output_file.as_deref(),
                debug,
            )?;
        }
        Commands::Parse { data_dir } => {
            if !std::path::Path::new(&data_dir).is_dir() {
                eprintln!("Error: directory not found at {}", data_dir);
                std::process::exit(1);
            }
            println!("Parsing JSON files from: {}", data_dir);
            parser::parse_jsons(&data_dir, &db_config, "info")?;
            parser::parse_jsons(&data_dir, &db_config, "live_chat")?;
        }
        Commands::Dbcheck => {
            search::db_check(&db_config)?;
        }
    }

    Ok(())
}
