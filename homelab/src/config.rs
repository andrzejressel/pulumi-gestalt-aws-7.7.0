use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Main configuration structure for AWS Batch + EventBridge Pipes solution
#[derive(Debug, Clone)]
pub struct Config {
    pub yaml_config: YamlConfig,
}

/// YAML configuration structure matching config.yaml
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YamlConfig {
    pub s3: S3Config,
    pub sqs: SqsConfig,
    pub pipes: EventBridgePipesConfig,
    pub batch: BatchConfig,
    pub ecr: EcrConfig,
    pub launch_template: LaunchTemplateConfig,
    pub project: ProjectConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: String,
    pub versioning: bool,
    pub lifecycle_days: u32,
    #[serde(rename = "forceDestroy")]
    pub force_destroy: bool,
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsConfig {
    pub queue_name: String,
    pub region: String,
    pub visibility_timeout: u32,
    pub message_retention_seconds: u32,
    pub dlq_enabled: bool,
    pub dlq_max_receive_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBridgePipesConfig {
    pub pipe_name: String,
    pub region: String,
    pub batch_size: u32,
    pub maximum_batching_window_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub compute_environment_name: String,
    pub job_queue_name: String,
    pub job_definition_name: String,
    pub region: String,
    pub compute_environment_type: String, // "ec2" or "fargate"
    pub ec2_config: Ec2Config,
    pub fargate_config: FargateConfig,
    pub ec2_spot_config: Ec2SpotConfig,
    pub fargate_spot_config: FargateSpotConfig,
    pub container_image: String,
    pub vcpus: f64,
    pub memory: u32,
    pub job_timeout_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ec2Config {
    pub instance_types: Vec<String>,
    pub allocation_strategy: String,
    pub min_vcpus: u32,
    pub max_vcpus: u32,
    pub desired_vcpus: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FargateConfig {
    pub platform_version: String,
    pub min_vcpus: u32,
    pub max_vcpus: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ec2SpotConfig {
    pub enabled: bool,
    pub bid_percentage: u32,
    pub instance_types: Vec<String>,
    pub allocation_strategy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FargateSpotConfig {
    pub enabled: bool,
    pub platform_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcrConfig {
    pub repository_name: String,
    pub region: String,
    pub force_destroy: bool,
    pub scan_on_push: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchTemplateConfig {
    pub name: String,
    pub root_volume_size: u32,
    pub volume_type: String,
    pub encrypted: bool,
    pub delete_on_termination: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub environment: String,
    pub owner: String,
    pub tags: HashMap<String, String>,
}

impl Config {
    /// Load configuration from config.yaml and environment variables
    pub fn load() -> Result<Self> {
        info!("🔧 Loading configuration from config.yaml and environment variables");

        // Load environment variables from .env file
        debug!("📋 Loading .env file into environment");
        // dotenv::dotenv().ok(); // Load .env file into environment (ignore errors if file doesn't exist)

        // Load YAML configuration from config.yaml
        debug!("📋 Reading config.yaml file");
        let yaml_content = std::fs::read_to_string("config.yaml")
            .with_context(|| "Failed to read config.yaml - ensure file exists in project root")?;

        debug!("🔧 Parsing YAML configuration");
        let mut yaml_config: YamlConfig = serde_yaml::from_str(&yaml_content)
            .with_context(|| "Failed to parse config.yaml - check YAML syntax and structure")?;

        // Expand environment variables in configuration
        debug!("🌐 Expanding environment variables in configuration");
        Self::expand_environment_variables(&mut yaml_config)?;

        // Generate unique resource names if they contain placeholders
        Self::generate_unique_names(&mut yaml_config)?;

        // Validate configuration
        debug!("✅ Validating configuration");
        Self::validate_config(&yaml_config)?;

        info!("✅ Configuration loaded and validated successfully");
        debug!(
            "📊 Config summary: S3 bucket: {}, Batch compute: {}",
            yaml_config.s3.bucket_name, yaml_config.batch.compute_environment_name
        );

        Ok(Config { yaml_config })
    }

    /// Expand environment variables in YAML configuration
    fn expand_environment_variables(config: &mut YamlConfig) -> Result<()> {
        info!("🌐 Expanding environment variables in configuration");

        // S3 configuration
        config.s3.bucket_name = Self::expand_env_var(&config.s3.bucket_name)?;
        config.s3.region = Self::expand_env_var(&config.s3.region)?;

        // SQS configuration
        config.sqs.queue_name = Self::expand_env_var(&config.sqs.queue_name)?;
        config.sqs.region = Self::expand_env_var(&config.sqs.region)?;

        // EventBridge Pipes configuration
        config.pipes.pipe_name = Self::expand_env_var(&config.pipes.pipe_name)?;
        config.pipes.region = Self::expand_env_var(&config.pipes.region)?;

        // Batch configuration
        config.batch.compute_environment_name =
            Self::expand_env_var(&config.batch.compute_environment_name)?;
        config.batch.job_queue_name = Self::expand_env_var(&config.batch.job_queue_name)?;
        config.batch.job_definition_name = Self::expand_env_var(&config.batch.job_definition_name)?;
        config.batch.region = Self::expand_env_var(&config.batch.region)?;
        config.batch.container_image = Self::expand_env_var(&config.batch.container_image)?;

        // ECR configuration
        config.ecr.repository_name = Self::expand_env_var(&config.ecr.repository_name)?;
        config.ecr.region = Self::expand_env_var(&config.ecr.region)?;

        // Project configuration
        config.project.name = Self::expand_env_var(&config.project.name)?;
        config.project.environment = Self::expand_env_var(&config.project.environment)?;
        config.project.owner = Self::expand_env_var(&config.project.owner)?;

        info!("✅ Environment variable expansion completed");
        Ok(())
    }

    /// Expand environment variables in a string (supports multiple variables)
    /// Skips RANDOM_SUFFIX placeholders which are handled by generate_unique_names()
    fn expand_env_var(value: &str) -> Result<String> {
        let mut result = value.to_string();

        // Find all ${VAR} patterns and replace them
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find("}") {
                let end = start + end;
                let var_name = &result[start + 2..end];
                debug!("🔍 Found variable pattern: {}", var_name);

                // Skip RANDOM_SUFFIX - it's handled by generate_unique_names(), not environment variables
                if var_name == "RANDOM_SUFFIX" {
                    debug!("⏭️ Skipping RANDOM_SUFFIX - will be processed by generate_unique_names()");
                    // Move past this variable to continue processing other variables in the string
                    let temp_result = result[..start].to_string() +
                                     &result[start..=end].to_string() +
                                     &result[end + 1..];
                    let remaining = &temp_result[end + 1..];
                    if remaining.contains("${") {
                        // Continue from after this RANDOM_SUFFIX to process other variables
                        result = result[..=end].to_string() + &Self::expand_env_var(&result[end + 1..])?;
                        break;
                    } else {
                        // No more variables to process
                        break;
                    }
                }

                debug!("🔍 Expanding environment variable: {}", var_name);
                let var_value = std::env::var(var_name).with_context(|| {
                    format!(
                        "Environment variable {} not found - check .env file",
                        var_name
                    )
                })?;

                result.replace_range(start..=end, &var_value);
            } else {
                // Malformed ${VAR without closing }
                return Err(anyhow::anyhow!(
                    "Malformed environment variable syntax: missing closing '}}' after '{}'",
                    &result[start..]
                ));
            }
        }

        debug!("✅ Expanded '{}' to '{}'", value, result);
        Ok(result)
    }

    /// Generate unique names for resources that contain placeholders
    fn generate_unique_names(config: &mut YamlConfig) -> Result<()> {
        info!("🎲 Generating unique resource names");

        // Generate single UUID for all resources to ensure consistency
        let suffix = Uuid::new_v4().to_string()[..8].to_string();
        info!("🔑 Generated unique suffix for all resources: {}", suffix);

        // Apply same suffix to all resources that need it
        if config.s3.bucket_name.contains("${RANDOM_SUFFIX}") {
            config.s3.bucket_name = config.s3.bucket_name.replace("${RANDOM_SUFFIX}", &suffix);
            info!(
                "📦 Generated unique S3 bucket name: {}",
                config.s3.bucket_name
            );
        }

        if config.sqs.queue_name.contains("${RANDOM_SUFFIX}") {
            config.sqs.queue_name = config.sqs.queue_name.replace("${RANDOM_SUFFIX}", &suffix);
            info!(
                "📬 Generated unique SQS queue name: {}",
                config.sqs.queue_name
            );
        }

        if config.ecr.repository_name.contains("${RANDOM_SUFFIX}") {
            config.ecr.repository_name = config.ecr.repository_name.replace("${RANDOM_SUFFIX}", &suffix);
            info!(
                "🐳 Generated unique ECR repository name: {}",
                config.ecr.repository_name
            );
        }

        Ok(())
    }

    /// Validate configuration values
    fn validate_config(config: &YamlConfig) -> Result<()> {
        info!("✅ Validating configuration parameters");

        // Validate S3 configuration
        if config.s3.bucket_name.is_empty() {
            return Err(anyhow::anyhow!("S3 bucket name cannot be empty"));
        }
        if config.s3.lifecycle_days == 0 {
            warn!("⚠️ S3 lifecycle_days is 0 - objects will not be automatically deleted");
        }

        // Validate SQS configuration
        if config.sqs.visibility_timeout < 30 {
            warn!(
                "⚠️ SQS visibility timeout is less than 30 seconds - may cause processing issues"
            );
        }
        if config.sqs.message_retention_seconds < 60 {
            return Err(anyhow::anyhow!(
                "SQS message retention must be at least 60 seconds"
            ));
        }

        // Validate Batch configuration
        if config.batch.ec2_config.max_vcpus < config.batch.ec2_config.min_vcpus {
            return Err(anyhow::anyhow!("Batch max_vcpus must be >= min_vcpus"));
        }
        if config.batch.memory < 512 {
            return Err(anyhow::anyhow!("Batch job memory must be at least 512 MB"));
        }
        if config.batch.vcpus <= 0.0 {
            return Err(anyhow::anyhow!("Batch job vCPUs must be > 0"));
        }

        // Validate compute environment type
        match config.batch.compute_environment_type.as_str() {
            "ec2" | "fargate" => {}
            _ => {
                return Err(anyhow::anyhow!(
                    "Batch compute_environment_type must be 'ec2' or 'fargate'"
                ))
            }
        }

        // Validate EC2 configuration if EC2 mode
        if config.batch.compute_environment_type == "ec2" {
            Self::validate_ec2_config(&config.batch.ec2_config)?;
        }

        // Validate Fargate configuration if Fargate mode
        if config.batch.compute_environment_type == "fargate" {
            Self::validate_fargate_config(&config.batch.fargate_config)?;
            Self::validate_fargate_job_config(&config.batch)?;
        }

        // Validate spot configurations (future use)
        Self::validate_spot_configs(&config.batch.ec2_spot_config, &config.batch.fargate_spot_config)?;

        info!("✅ Configuration validation completed successfully");
        Ok(())
    }

    /// Validate EC2 configuration and ARM64 instance types
    fn validate_ec2_config(ec2_config: &Ec2Config) -> Result<()> {
        debug!("🔍 Validating EC2 configuration");

        // Validate min/max vCPUs
        if ec2_config.max_vcpus < ec2_config.min_vcpus {
            return Err(anyhow::anyhow!("EC2 max_vcpus must be >= min_vcpus"));
        }

        // Validate explicitly supported ARM64 instance types
        let supported_arm64_families = vec![
            "c6g", "c7g", // Compute-optimized Graviton
            "m6g", "m7g", // General purpose Graviton
            "r6g", "r7g", // Memory-optimized Graviton
            "x2gd",       // Memory-optimized with NVMe SSD
        ];

        for instance_type in &ec2_config.instance_types {
            let instance_family = instance_type.split('.').next().unwrap_or("");
            if !supported_arm64_families.contains(&instance_family) {
                warn!(
                    "⚠️ Instance type '{}' is not in explicitly supported ARM64 families: {:?}",
                    instance_type, supported_arm64_families
                );
            }
        }

        // Validate allocation strategy
        let valid_strategies = vec![
            "BEST_FIT",
            "BEST_FIT_PROGRESSIVE",
            "SPOT_CAPACITY_OPTIMIZED"
        ];
        if !valid_strategies.contains(&ec2_config.allocation_strategy.as_str()) {
            return Err(anyhow::anyhow!(
                "EC2 allocation_strategy must be one of: {:?}",
                valid_strategies
            ));
        }

        info!("✅ EC2 configuration validated successfully");
        Ok(())
    }

    /// Validate Fargate configuration
    fn validate_fargate_config(fargate_config: &FargateConfig) -> Result<()> {
        debug!("🔍 Validating Fargate configuration");

        // Validate min/max vCPUs (Fargate-specific limits)
        if fargate_config.min_vcpus != 0 {
            warn!("⚠️ Fargate min_vcpus should be 0 (Fargate doesn't support persistent capacity)");
        }
        if fargate_config.max_vcpus > 1000 {
            return Err(anyhow::anyhow!("Fargate max_vcpus cannot exceed 1000"));
        }

        // Validate platform version
        let valid_platform_versions = vec!["LATEST", "1.3.0", "1.4.0"];
        if !valid_platform_versions.contains(&fargate_config.platform_version.as_str()) {
            warn!(
                "⚠️ Fargate platform_version '{}' may not be supported. Recommended: {:?}",
                fargate_config.platform_version, valid_platform_versions
            );
        }

        info!("✅ Fargate configuration validated successfully");
        Ok(())
    }

    /// Validate Fargate job configuration (vCPU/memory combinations)
    fn validate_fargate_job_config(batch_config: &BatchConfig) -> Result<()> {
        debug!("🔍 Validating Fargate job vCPU/memory configuration");

        let vcpus = batch_config.vcpus;
        let memory = batch_config.memory;

        // Validate vCPU values (Fargate supports: 0.25, 0.5, 1, 2, 4, 8, 16)
        let valid_vcpus = vec![0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0];
        if !valid_vcpus.contains(&vcpus) {
            return Err(anyhow::anyhow!(
                "Fargate vCPUs must be one of: {:?}, got: {}",
                valid_vcpus, vcpus
            ));
        }

        // Validate vCPU/memory combinations according to AWS Fargate specifications
        let valid_combination = match vcpus {
            0.25 => (512..=2048).contains(&memory),     // 0.25 vCPU: 512-2048 MB
            0.5 => (1024..=4096).contains(&memory),     // 0.5 vCPU: 1024-4096 MB
            1.0 => (2048..=8192).contains(&memory),     // 1 vCPU: 2048-8192 MB
            2.0 => (4096..=16384).contains(&memory),    // 2 vCPU: 4096-16384 MB
            4.0 => (8192..=30720).contains(&memory),    // 4 vCPU: 8192-30720 MB
            8.0 => (16384..=61440).contains(&memory),   // 8 vCPU: 16384-61440 MB
            16.0 => (32768..=122880).contains(&memory), // 16 vCPU: 32768-122880 MB
            _ => false,
        };

        if !valid_combination {
            let memory_range = match vcpus {
                0.25 => "512-2048 MB",
                0.5 => "1024-4096 MB",
                1.0 => "2048-8192 MB",
                2.0 => "4096-16384 MB",
                4.0 => "8192-30720 MB",
                8.0 => "16384-61440 MB",
                16.0 => "32768-122880 MB",
                _ => "invalid",
            };
            return Err(anyhow::anyhow!(
                "Invalid Fargate vCPU/memory combination: {} vCPU requires {} memory, got {} MB",
                vcpus, memory_range, memory
            ));
        }

        info!("✅ Fargate job configuration validated: {} vCPU + {} MB", vcpus, memory);
        Ok(())
    }

    /// Validate spot configurations (future use validation)
    fn validate_spot_configs(
        ec2_spot_config: &Ec2SpotConfig,
        fargate_spot_config: &FargateSpotConfig,
    ) -> Result<()> {
        debug!("🔍 Validating spot configurations (future use)");

        // Validate EC2 Spot configuration
        if ec2_spot_config.enabled {
            warn!("⚠️ EC2 Spot is not yet implemented - enabled flag will be ignored");
        }
        if ec2_spot_config.bid_percentage > 100 {
            return Err(anyhow::anyhow!("EC2 Spot bid_percentage cannot exceed 100"));
        }

        // Validate Fargate Spot configuration
        if fargate_spot_config.enabled {
            warn!("⚠️ Fargate Spot is not available yet - enabled flag will be ignored");
        }

        info!("✅ Spot configurations validated (future use prepared)");
        Ok(())
    }

    /// Get project tags including default ones
    pub fn get_project_tags(&self) -> HashMap<String, String> {
        let mut tags = self.yaml_config.project.tags.clone();

        // Add default tags
        tags.insert("Project".to_string(), self.yaml_config.project.name.clone());
        tags.insert(
            "Environment".to_string(),
            self.yaml_config.project.environment.clone(),
        );
        tags.insert("Owner".to_string(), self.yaml_config.project.owner.clone());
        tags.insert("ManagedBy".to_string(), "PulumiGestalt".to_string());
        tags.insert("Service".to_string(), "AWSBatchImageBuilder".to_string());

        debug!("🏷️ Project tags: {:?}", tags);
        tags
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_expand_single_env_var() {
        env::set_var("TEST_VAR", "test_value");
        let result = Config::expand_env_var("${TEST_VAR}").unwrap();
        assert_eq!(result, "test_value");
        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_expand_multiple_env_vars() {
        env::set_var("PROJECT_NAME", "test-project");
        // Note: RANDOM_SUFFIX is not set as env var - it should be left unexpanded

        let result = Config::expand_env_var("${PROJECT_NAME}-ecr-${RANDOM_SUFFIX}").unwrap();
        assert_eq!(result, "test-project-ecr-${RANDOM_SUFFIX}"); // RANDOM_SUFFIX left unexpanded

        env::remove_var("PROJECT_NAME");
    }

    #[test]
    fn test_expand_env_var_with_text() {
        env::set_var("REGION", "us-east-1");
        let result = Config::expand_env_var("bucket-${REGION}-suffix").unwrap();
        assert_eq!(result, "bucket-us-east-1-suffix");
        env::remove_var("REGION");
    }

    #[test]
    fn test_expand_no_env_vars() {
        let result = Config::expand_env_var("plain-string").unwrap();
        assert_eq!(result, "plain-string");
    }

    #[test]
    fn test_expand_missing_env_var() {
        let result = Config::expand_env_var("${MISSING_VAR}");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MISSING_VAR not found"));
    }

    #[test]
    fn test_expand_malformed_env_var_no_closing_brace() {
        let result = Config::expand_env_var("${UNCLOSED_VAR");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing closing '}'"));
    }

    #[test]
    fn test_expand_empty_env_var() {
        env::set_var("EMPTY_VAR", "");
        let result = Config::expand_env_var("prefix-${EMPTY_VAR}-suffix").unwrap();
        assert_eq!(result, "prefix--suffix");
        env::remove_var("EMPTY_VAR");
    }

    #[test]
    fn test_expand_nested_braces() {
        env::set_var("OUTER", "value");
        let result = Config::expand_env_var("${OUTER}").unwrap();
        assert_eq!(result, "value");
        env::remove_var("OUTER");
    }

    #[test]
    fn test_expand_consecutive_env_vars() {
        env::set_var("VAR1", "first");
        env::set_var("VAR2", "second");
        let result = Config::expand_env_var("${VAR1}${VAR2}").unwrap();
        assert_eq!(result, "firstsecond");
        env::remove_var("VAR1");
        env::remove_var("VAR2");
    }

    #[test]
    fn test_specific_ecr_repository_bug() {
        // This test catches the original bug - RANDOM_SUFFIX should not be expanded as env var
        env::set_var("PROJECT_NAME", "comfyui-batch-builder");
        // Note: RANDOM_SUFFIX is not set as env var - it should be left unexpanded

        let result = Config::expand_env_var("${PROJECT_NAME}-ecr-${RANDOM_SUFFIX}").unwrap();
        assert_eq!(result, "comfyui-batch-builder-ecr-${RANDOM_SUFFIX}"); // RANDOM_SUFFIX left unexpanded

        env::remove_var("PROJECT_NAME");
    }

    #[test]
    fn test_expand_env_var_case_sensitive() {
        env::set_var("TestVar", "correct");
        env::set_var("TESTVAR", "wrong");

        let result = Config::expand_env_var("${TestVar}").unwrap();
        assert_eq!(result, "correct");

        env::remove_var("TestVar");
        env::remove_var("TESTVAR");
    }

    #[test]
    fn test_expand_with_special_characters() {
        env::set_var("SPECIAL_VAR", "value-with_special.chars");
        let result = Config::expand_env_var("${SPECIAL_VAR}").unwrap();
        assert_eq!(result, "value-with_special.chars");
        env::remove_var("SPECIAL_VAR");
    }

    // Configuration loading tests
    #[test]
    fn test_env_var_expansion_in_config() {
        // Set up test environment variables
        env::set_var("PROJECT_NAME", "test-project");
        env::set_var("AWS_DEFAULT_REGION", "us-test-1");
        env::set_var("PROJECT_OWNER", "test-team");
        env::set_var("ENVIRONMENT", "test");
        // Note: RANDOM_SUFFIX is not set as env var - it should be left unexpanded

        // Test the specific ECR repository name - RANDOM_SUFFIX should remain unexpanded
        let result = Config::expand_env_var("${PROJECT_NAME}-ecr-${RANDOM_SUFFIX}").unwrap();
        assert_eq!(result, "test-project-ecr-${RANDOM_SUFFIX}"); // RANDOM_SUFFIX left unexpanded

        // Test S3 bucket name pattern - RANDOM_SUFFIX should remain unexpanded
        let result = Config::expand_env_var("comfyui-batch-source-${RANDOM_SUFFIX}").unwrap();
        assert_eq!(result, "comfyui-batch-source-${RANDOM_SUFFIX}"); // RANDOM_SUFFIX left unexpanded

        // Clean up
        env::remove_var("PROJECT_NAME");
        env::remove_var("AWS_DEFAULT_REGION");
        env::remove_var("PROJECT_OWNER");
        env::remove_var("ENVIRONMENT");
    }

    #[test]
    fn test_random_suffix_placeholder_handling() {
        // Test that RANDOM_SUFFIX placeholders are correctly left unexpanded by expand_env_var
        // and would be processed later by generate_unique_names

        env::set_var("PROJECT_NAME", "test-project");
        env::set_var("AWS_DEFAULT_REGION", "us-test-1");

        // Test all the patterns from config.yaml that contain RANDOM_SUFFIX
        let s3_pattern = "comfyui-batch-source-${RANDOM_SUFFIX}";
        let sqs_pattern = "comfyui-batch-queue-${RANDOM_SUFFIX}";
        let ecr_pattern = "${PROJECT_NAME}-ecr-${RANDOM_SUFFIX}";

        // These should expand PROJECT_NAME but leave RANDOM_SUFFIX unexpanded
        let s3_result = Config::expand_env_var(s3_pattern).unwrap();
        assert_eq!(s3_result, "comfyui-batch-source-${RANDOM_SUFFIX}");

        let sqs_result = Config::expand_env_var(sqs_pattern).unwrap();
        assert_eq!(sqs_result, "comfyui-batch-queue-${RANDOM_SUFFIX}");

        let ecr_result = Config::expand_env_var(ecr_pattern).unwrap();
        assert_eq!(ecr_result, "test-project-ecr-${RANDOM_SUFFIX}");

        env::remove_var("PROJECT_NAME");
        env::remove_var("AWS_DEFAULT_REGION");
    }

    #[test]
    fn test_validate_required_env_vars() {
        // This test ensures we catch missing required environment variables
        let result = Config::expand_env_var("${DEFINITELY_MISSING_VAR}");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("DEFINITELY_MISSING_VAR"));
        assert!(error_msg.contains("not found"));
    }

    #[test]
    fn test_expand_env_var_edge_cases() {
        // Test various edge cases that could break parsing

        // Empty string
        let result = Config::expand_env_var("").unwrap();
        assert_eq!(result, "");

        // Just ${}
        let result = Config::expand_env_var("${}");
        assert!(result.is_err());

        // Multiple consecutive braces
        env::set_var("TEST_VAR", "value");
        let result = Config::expand_env_var("${TEST_VAR}${TEST_VAR}${TEST_VAR}").unwrap();
        assert_eq!(result, "valuevaluevalue");
        env::remove_var("TEST_VAR");

        // Mixed with regular text
        env::set_var("PREFIX", "pre");
        env::set_var("SUFFIX", "suf");
        let result = Config::expand_env_var("start-${PREFIX}-middle-${SUFFIX}-end").unwrap();
        assert_eq!(result, "start-pre-middle-suf-end");
        env::remove_var("PREFIX");
        env::remove_var("SUFFIX");
    }

    // Property-based tests for fuzzing
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fuzz_env_var_expansion_should_not_panic(s in "\\PC*") {
            // This test ensures that no input string causes a panic
            let _ = Config::expand_env_var(&s);
        }

        #[test]
        fn fuzz_env_var_expansion_valid_chars(s in "[a-zA-Z0-9_-]*") {
            // Test with valid environment variable characters
            let _ = Config::expand_env_var(&s);
        }

        #[test]
        fn fuzz_env_var_expansion_with_braces(
            prefix in "[a-zA-Z0-9_-]*",
            var_name in "[A-Z_][A-Z0-9_]*",
            suffix in "[a-zA-Z0-9_-]*"
        ) {
            // Test with well-formed environment variable patterns
            let input = format!("{}${{{}}}{}", prefix, var_name, suffix);

            // Set a test environment variable
            env::set_var(&var_name, "test_value");

            let result = Config::expand_env_var(&input);

            // Clean up
            env::remove_var(&var_name);

            // Should either succeed or fail gracefully (no panic)
            match result {
                Ok(expanded) => {
                    assert!(expanded.contains("test_value"));
                }
                Err(_) => {
                    // Acceptable failure case
                }
            }
        }

        #[test]
        fn fuzz_multiple_env_vars(
            var1 in "[A-Z_][A-Z0-9_]*",
            var2 in "[A-Z_][A-Z0-9_]*",
            separator in "[a-zA-Z0-9_-]*"
        ) {
            prop_assume!(var1 != var2); // Ensure different variable names
            prop_assume!(var1.len() >= 2 && var2.len() >= 2); // Ensure meaningful variable names

            let input = format!("${{{}}}{}{{{}}}", var1, separator, var2);

            // Set test environment variables
            env::set_var(&var1, "value1");
            env::set_var(&var2, "value2");

            let result = Config::expand_env_var(&input);

            // Clean up immediately after calling expand_env_var but before assertions
            env::remove_var(&var1);
            env::remove_var(&var2);

            // Should successfully expand both variables
            prop_assert!(result.is_ok(), "Failed to expand: {} -> {:?}", input, result);
        }

        #[test]
        fn fuzz_malformed_syntax(
            prefix in "[a-zA-Z0-9_-]*",
            middle in "[a-zA-Z0-9_${}]*",
            suffix in "[a-zA-Z0-9_-]*"
        ) {
            // Generate potentially malformed variable syntax
            let input = format!("{}${{{}{}}}", prefix, middle, suffix);

            let result = Config::expand_env_var(&input);

            // Should either succeed or fail gracefully (no panics)
            match result {
                Ok(_) => {}, // Valid expansion
                Err(_) => {}, // Expected error for malformed syntax
            }
        }

        #[test]
        fn fuzz_nested_and_complex_patterns(
            text in "[a-zA-Z0-9_-]{0,50}",
            var_name in "[A-Z_][A-Z0-9_]*",
            extra_text in "[a-zA-Z0-9_.-]{0,30}"
        ) {
            // Test various complex patterns
            let patterns = vec![
                format!("{}${{{}}}{}", text, var_name, extra_text),
                format!("${{{}}}{}${{{}}}", var_name, text, var_name),
                format!("prefix-${{{}}}-middle-${{{}}}-suffix", var_name, var_name),
            ];

            for pattern in patterns {
                env::set_var(&var_name, "test_value");

                let result = Config::expand_env_var(&pattern);

                env::remove_var(&var_name);

                // Should not panic
                let _ = result;
            }
        }

        #[test]
        fn fuzz_edge_case_variable_names(
            var_name in prop::option::of("[A-Z_][A-Z0-9_]*"),
            bracket_count in 1..5usize
        ) {
            let input = match &var_name {
                Some(name) => {
                    // Valid variable with varying bracket patterns
                    let mut brackets = String::new();
                    for _ in 0..bracket_count {
                        brackets.push_str("${{}}");
                    }
                    format!("${{{}}}{}", name, brackets)
                },
                None => {
                    // Edge case: empty or invalid variable names
                    let mut pattern = String::from("${{");
                    for _ in 0..bracket_count {
                        pattern.push('}');
                    }
                    pattern
                }
            };

            if let Some(ref name) = var_name {
                env::set_var(name, "test_value");
            }

            let result = Config::expand_env_var(&input);

            if let Some(ref name) = var_name {
                env::remove_var(name);
            }

            // Should handle gracefully without panic
            let _ = result;
        }
    }

    // Integration tests for full configuration scenarios
    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use std::env;

        #[test]
        fn test_env_var_expansion_in_config_fields() {
            // Test that environment variable expansion works with key configuration fields
            env::set_var("INTEGRATION_PROJECT", "test-integration");
            env::set_var("INTEGRATION_ENV", "staging");
            env::set_var("INTEGRATION_REGION", "us-west-2");

            // Test individual field expansions
            assert_eq!(
                Config::expand_env_var("${INTEGRATION_PROJECT}-source").unwrap(),
                "test-integration-source"
            );

            assert_eq!(
                Config::expand_env_var("${INTEGRATION_PROJECT}-${INTEGRATION_ENV}-compute")
                    .unwrap(),
                "test-integration-staging-compute"
            );

            assert_eq!(
                Config::expand_env_var("${INTEGRATION_REGION}").unwrap(),
                "us-west-2"
            );

            // Test multiple variable expansion in single string
            assert_eq!(
                Config::expand_env_var("${INTEGRATION_PROJECT}-ecr-${INTEGRATION_REGION}").unwrap(),
                "test-integration-ecr-us-west-2"
            );

            // Clean up
            env::remove_var("INTEGRATION_PROJECT");
            env::remove_var("INTEGRATION_ENV");
            env::remove_var("INTEGRATION_REGION");
        }

        #[test]
        fn test_complex_nested_env_expansion() {
            env::set_var("BASE_NAME", "complex");
            env::set_var("ENV_TYPE", "staging");
            env::set_var("REGION_CODE", "us");
            env::set_var("ZONE", "1a");

            // Test multiple combinations of environment variables
            assert_eq!(
                Config::expand_env_var("${BASE_NAME}-${ENV_TYPE}").unwrap(),
                "complex-staging"
            );

            assert_eq!(
                Config::expand_env_var("${BASE_NAME}-${ENV_TYPE}-compute-${REGION_CODE}-${ZONE}")
                    .unwrap(),
                "complex-staging-compute-us-1a"
            );

            assert_eq!(
                Config::expand_env_var("${REGION_CODE}-east-${ZONE}").unwrap(),
                "us-east-1a"
            );

            assert_eq!(
                Config::expand_env_var("${BASE_NAME}-${ENV_TYPE}-deployment").unwrap(),
                "complex-staging-deployment"
            );

            env::remove_var("BASE_NAME");
            env::remove_var("ENV_TYPE");
            env::remove_var("REGION_CODE");
            env::remove_var("ZONE");
        }

        #[test]
        fn test_missing_env_var_in_expansion() {
            let result = Config::expand_env_var("${MISSING_VAR_TEST}");
            assert!(
                result.is_err(),
                "Should fail when environment variable is missing"
            );
        }

        #[test]
        fn test_static_strings_pass_through() {
            assert_eq!(
                Config::expand_env_var("static-project").unwrap(),
                "static-project"
            );

            assert_eq!(Config::expand_env_var("production").unwrap(), "production");

            assert_eq!(
                Config::expand_env_var("complex-static-string-with-no-vars").unwrap(),
                "complex-static-string-with-no-vars"
            );
        }

        #[test]
        fn test_malformed_env_var_syntax() {
            let result = Config::expand_env_var("${UNCLOSED_VAR");
            assert!(result.is_err(), "Should fail with malformed syntax");

            let result2 = Config::expand_env_var("UNBRACED_VAR}");
            assert!(
                result2.is_ok(),
                "Should pass through invalid syntax as literal text"
            );
            assert_eq!(result2.unwrap(), "UNBRACED_VAR}");
        }

        #[test]
        fn test_real_world_config_patterns() {
            // Test patterns commonly used in AWS infrastructure
            env::set_var("TEST_PROJECT", "myapp");
            env::set_var("TEST_ENVIRONMENT", "prod");
            env::set_var("TEST_REGION", "us-east-1");
            env::set_var("TEST_SUFFIX", "abc123");

            // S3 bucket naming pattern
            assert_eq!(
                Config::expand_env_var("${TEST_PROJECT}-${TEST_ENVIRONMENT}-source-${TEST_SUFFIX}")
                    .unwrap(),
                "myapp-prod-source-abc123"
            );

            // ECR repository pattern
            assert_eq!(
                Config::expand_env_var("${TEST_PROJECT}-ecr-${TEST_SUFFIX}").unwrap(),
                "myapp-ecr-abc123"
            );

            // Batch compute environment pattern
            assert_eq!(
                Config::expand_env_var("${TEST_PROJECT}-${TEST_ENVIRONMENT}-compute").unwrap(),
                "myapp-prod-compute"
            );

            // EventBridge pipe pattern
            assert_eq!(
                Config::expand_env_var("${TEST_PROJECT}-pipe-${TEST_REGION}").unwrap(),
                "myapp-pipe-us-east-1"
            );

            env::remove_var("TEST_PROJECT");
            env::remove_var("TEST_ENVIRONMENT");
            env::remove_var("TEST_REGION");
            env::remove_var("TEST_SUFFIX");
        }
    }
}
