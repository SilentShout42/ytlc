mod parser;
mod search;

use clap::{Parser, Subcommand};

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub dbname: String,
    pub user: String,
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
}

impl DbConfig {
    pub fn conn_string(&self) -> String {
        // Explicit password field takes precedence, then libpq env vars
        let pw = self
            .password
            .clone()
            .or_else(|| std::env::var("PGPASSWORD").ok())
            .or_else(|| std::env::var("POSTGRES_PASSWORD").ok());

        match pw {
            Some(p) => format!(
                "host={} user={} dbname={} port={} password={}",
                self.host, self.user, self.dbname, self.port, p
            ),
            None => format!(
                "host={} user={} dbname={} port={}",
                self.host, self.user, self.dbname, self.port
            ),
        }
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

    #[command(about = "Parse JSON files and load into PostgreSQL.")]
    Parse {
        #[arg(help = "Directory containing .info.json and .live_chat.json files.")]
        data_dir: String,
    },

    #[command(about = "Test the database connection and show basic stats.")]
    Dbcheck,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let db_config = DbConfig {
        dbname: "ytlc".to_string(),
        user: "ytlc".to_string(),
        host: "localhost".to_string(),
        port: 5432,
        password: None, // resolved from PGPASSWORD / POSTGRES_PASSWORD at connection time
    };

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
            parser::parse_jsons_to_postgres(&data_dir, &db_config, "info")?;
            parser::parse_jsons_to_postgres(&data_dir, &db_config, "live_chat")?;
        }
        Commands::Dbcheck => {
            search::db_check(&db_config)?;
        }
    }

    Ok(())
}
