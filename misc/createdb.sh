#!/usr/bin/env bash
set -euo pipefail

# Parameterize the database and user name
db_name="ytlc"
user_name="ytlc"

# Generate a strong random password for the user
generated_password=$(openssl rand -base64 16)

# Output the commands instead of running them
cat <<EOF
DO
\$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db_name') THEN
        CREATE DATABASE $db_name;
    END IF;
END
\$$;

DO
\$$
BEGIN
    BEGIN
        CREATE USER $user_name WITH ENCRYPTED PASSWORD '$generated_password';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'User already exists, updating password.';
        ALTER USER $user_name WITH ENCRYPTED PASSWORD '$generated_password';
    END;
    GRANT ALL PRIVILEGES ON DATABASE $db_name TO $user_name;
    ALTER DATABASE $db_name OWNER TO $user_name;
    GRANT USAGE, CREATE ON SCHEMA public TO $user_name;
END
\$$;
EOF

# Output the generated password to stderr
echo "Generated password for user '$user_name': $generated_password" >&2
