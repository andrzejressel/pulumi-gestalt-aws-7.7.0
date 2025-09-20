use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    pulumi_gestalt_build::generate_from_schema_with_filter(
        Path::new("./aws-modified.json"),
        &[
            "ec2",
            "pipes",
            "s3",
            "sqs",
            "iam",
            "ecrpublic",
            "cloudwatch",
            "batch",
            "vpc",
        ],
    )?;
    Ok(())
}
