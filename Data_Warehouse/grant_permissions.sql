-- We have to grant permissions to the lambda functions that have to access redshift data

CREATE USER "IAMR:lambda_function_iam_role" WITH PASSWORD '*****';
GRANT USAGE ON SCHEMA "staging-tables" TO "IAMR:lambda_function_iam_role";
GRANT USAGE ON SCHEMA "fact_dim_tables" TO "IAMR:lambda_function_iam_role";

GRANT ALL ON ALL TABLES IN SCHEMA "fact_dim_tables" TO "IAMR:lambda_function_iam_role";
GRANT ALL ON ALL TABLES IN SCHEMA "staging-tables" TO "IAMR:lambda_function_iam_role";

--Note:  We can give restricted grants as well for simplicity given all check Taxi_aws projects for speicific garnts 
