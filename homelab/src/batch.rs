use anyhow::Result;
use log::{debug, info};
use pulumi_gestalt_rust::*;
use std::collections::HashMap;

use crate::config::Config;
use crate::vpc::VpcInfrastructure;


// Import AWS types for Pulumi Gestalt resource creation

pub struct BatchInfrastructure {
    pub compute_environment: crate::aws::batch::compute_environment::ComputeEnvironmentResult,
    pub job_queue: crate::aws::batch::job_queue::JobQueueResult,
    pub job_definition: crate::aws::batch::job_definition::JobDefinitionResult,
    pub instance_profile: crate::aws::iam::instance_profile::InstanceProfileResult,
    pub execution_role: crate::aws::iam::role::RoleResult,
}

/// Creates AWS Batch infrastructure including compute environment, job queue, and job definition

/// Creates AWS Batch infrastructure with VPC integration
pub fn create_batch_infrastructure_with_vpc(
    context: &Context,
    config: &Config,
    vpc_infra: &VpcInfrastructure,
    ecr_repository: &crate::aws::ecrpublic::repository::RepositoryResult,
) -> Result<BatchInfrastructure> {
    info!("⚡ Creating AWS Batch infrastructure with VPC integration");
    debug!(
        "📋 Batch config with VPC: compute_environment_type={}, job_memory={}MB",
        config.yaml_config.batch.compute_environment_type,
        config.yaml_config.batch.memory
    );
    debug!("🌐 Using VPC infrastructure: subnets and security groups configured");

    let tags = config.get_project_tags();

    // Create Launch Template for larger EBS volumes
    info!("💾 Creating EC2 Launch Template with larger EBS volume");
    let launch_template = create_launch_template(context, config, &tags)?;

    // Create IAM roles for Batch (excluding service role - using AWS service-linked role)
    info!("🔐 Creating IAM roles for AWS Batch");
    let (_instance_role, instance_profile, execution_role) =
        create_batch_iam_roles(context, config, &tags)?;

    // Create Batch Compute Environment with VPC configuration (switch between EC2 and Fargate)
    info!("💻 Creating Batch compute environment with VPC integration");
    let compute_environment = match config.yaml_config.batch.compute_environment_type.as_str() {
        "ec2" => {
            info!("🔧 Creating EC2 MANAGED compute environment with launch template");
            create_ec2_managed_compute_environment_with_vpc(
                context,
                config,
                &instance_profile,
                &launch_template,
                vpc_infra,
                &tags
            )?
        },
        "fargate" => {
            info!("🔧 Creating Fargate MANAGED compute environment");
            create_fargate_managed_compute_environment_with_vpc(
                context,
                config,
                vpc_infra,
                &tags
            )?
        },
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported compute environment type: {}",
                config.yaml_config.batch.compute_environment_type
            ));
        }
    };

    // Create Batch Job Queue
    info!("📋 Creating Batch job queue");
    let job_queue = create_job_queue(context, config, &compute_environment, &tags)?;

    // Create Batch Job Definition
    info!("📝 Creating Batch job definition");
    let job_definition = create_job_definition(context, config, &execution_role, ecr_repository, &tags)?;

    info!("✅ AWS Batch infrastructure with VPC integration created successfully");

    Ok(BatchInfrastructure {
        compute_environment,
        job_queue,
        job_definition,
        instance_profile,
        execution_role,
    })
}

/// Creates EC2 Launch Template with EBS volume configuration for larger disk space
fn create_launch_template(
    context: &Context,
    config: &Config,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::launch_template::LaunchTemplateResult> {
    info!("💾 Creating EC2 Launch Template with {}GB root volume", config.yaml_config.launch_template.root_volume_size);
    debug!("📋 Launch template config: volume_type={}, encrypted={}",
           config.yaml_config.launch_template.volume_type,
           config.yaml_config.launch_template.encrypted);

    let launch_template = crate::aws::ec2::launch_template::create(
        context,
        "batch-launch-template",
        crate::aws::ec2::launch_template::LaunchTemplateArgs::builder()
            .name(config.yaml_config.launch_template.name.clone())
            .block_device_mappings(vec![
                crate::aws::types::ec2::LaunchTemplateBlockDeviceMapping::builder()
                    .device_name("/dev/xvda")  // Root device for EC2 instances
                    .ebs(
                        crate::aws::types::ec2::LaunchTemplateBlockDeviceMappingEbs::builder()
                            .volume_size(config.yaml_config.launch_template.root_volume_size as i32)
                            .volume_type(config.yaml_config.launch_template.volume_type.clone())
                            .delete_on_termination(config.yaml_config.launch_template.delete_on_termination.to_string())
                            .encrypted(config.yaml_config.launch_template.encrypted.to_string())
                            .build_struct()
                    )
                    .build_struct()
            ])
            .tags(tags.clone())
            .build_struct()
    );

    debug!("✅ Launch template created with {}GB EBS volume", config.yaml_config.launch_template.root_volume_size);
    Ok(launch_template)
}

/// Creates IAM roles required for AWS Batch operations (excluding service role - AWS manages that)
fn create_batch_iam_roles(
    context: &Context,
    config: &Config,
    tags: &HashMap<String, String>,
) -> Result<(
    crate::aws::iam::role::RoleResult,
    crate::aws::iam::instance_profile::InstanceProfileResult,
    crate::aws::iam::role::RoleResult,
)> {
    info!("🔐 Creating IAM roles for AWS Batch operations");

    // Service role REMOVED: AWS Batch will use service-linked role automatically
    debug!("👤 No custom service role - AWS Batch will use AWSServiceRoleForBatch service-linked role");

    // Instance role for EC2 instances in compute environment
    debug!("💻 Creating EC2 instance role for Batch compute environment");
    let instance_role = crate::aws::iam::role::create(
        context,
        "batch-instance-role",
        crate::aws::iam::role::RoleArgs::builder()
            .name(format!(
                "{}-batch-instance-role",
                config.yaml_config.project.name
            ))
            .assume_role_policy(
                include_str!("../config/iam/ec2-instance-assume-role-policy.json").to_string(),
            )
            .tags(tags.clone())
            .build_struct(),
    );

    // Attach required policies to instance role
    debug!("📎 Attaching policies to instance role");

    // ECS instance role for container management
    let _instance_ecs_policy = crate::aws::iam::role_policy_attachment::create(
        context,
        "batch-instance-ecs-policy",
        crate::aws::iam::role_policy_attachment::RolePolicyAttachmentArgs::builder()
            .role(instance_role.name.clone())
            .policy_arn(
                "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
                    .to_string(),
            )
            .build_struct(),
    );

    // ECR access for pulling/pushing Docker images
    let _instance_ecr_policy = crate::aws::iam::role_policy_attachment::create(
        context,
        "batch-instance-ecr-policy",
        crate::aws::iam::role_policy_attachment::RolePolicyAttachmentArgs::builder()
            .role(instance_role.name.clone())
            .policy_arn("arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess".to_string())
            .build_struct(),
    );

    // S3 access for downloading source code
    let _instance_s3_policy = crate::aws::iam::role_policy_attachment::create(
        context,
        "batch-instance-s3-policy",
        crate::aws::iam::role_policy_attachment::RolePolicyAttachmentArgs::builder()
            .role(instance_role.name.clone())
            .policy_arn("arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess".to_string())
            .build_struct(),
    );

    // Create instance profile for EC2 instances
    debug!("👥 Creating instance profile");
    let instance_profile = crate::aws::iam::instance_profile::create(
        context,
        "batch-instance-profile",
        crate::aws::iam::instance_profile::InstanceProfileArgs::builder()
            .name(format!(
                "{}-batch-instance-profile",
                config.yaml_config.project.name
            ))
            .role(instance_role.name.clone())
            .tags(tags.clone())
            .build_struct(),
    );

    // Fargate execution role for ECS task execution (required for Fargate jobs)
    debug!("🐳 Creating Fargate execution role for ECS task execution");
    let execution_role = crate::aws::iam::role::create(
        context,
        "fargate-execution-role",
        crate::aws::iam::role::RoleArgs::builder()
            .name(format!(
                "{}-fargate-execution-role",
                config.yaml_config.project.name
            ))
            .assume_role_policy(
                include_str!("../config/iam/ecs-task-execution-assume-role-policy.json").to_string(),
            )
            .tags(tags.clone())
            .build_struct(),
    );

    // Attach AWS managed policy for ECS task execution
    debug!("📎 Attaching ECS task execution policy to execution role");
    let _execution_ecs_policy = crate::aws::iam::role_policy_attachment::create(
        context,
        "execution-ecs-policy",
        crate::aws::iam::role_policy_attachment::RolePolicyAttachmentArgs::builder()
            .role(execution_role.name.clone())
            .policy_arn("arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy".to_string())
            .build_struct(),
    );

    // Attach custom policy for S3 and ECR Public access
    debug!("📎 Attaching custom S3 and ECR Public policy to execution role");
    let permissions_policy_template = include_str!("../config/iam/execution-role-permissions-policy.json");
    let permissions_policy = permissions_policy_template.replace("${S3_BUCKET_NAME}", &config.yaml_config.s3.bucket_name);

    let _execution_permissions_policy = crate::aws::iam::role_policy::create(
        context,
        "execution-permissions-policy",
        crate::aws::iam::role_policy::RolePolicyArgs::builder()
            .role(execution_role.name.clone())
            .policy(permissions_policy)
            .build_struct(),
    );

    info!("✅ IAM roles created successfully (excluding service role - AWS manages that)");
    Ok((instance_role, instance_profile, execution_role))
}

/// Creates AWS Batch compute environment

/// Creates AWS Batch EC2 MANAGED compute environment with VPC integration and ARM64 instances
fn create_ec2_managed_compute_environment_with_vpc(
    context: &Context,
    config: &Config,
    instance_profile: &crate::aws::iam::instance_profile::InstanceProfileResult,
    launch_template: &crate::aws::ec2::launch_template::LaunchTemplateResult,
    vpc_infra: &VpcInfrastructure,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::batch::compute_environment::ComputeEnvironmentResult> {
    info!("💻 Creating EC2 MANAGED compute environment with ARM64 instances");
    debug!(
        "🌐 VPC config: using public subnet and security group for EC2 instances (cost-optimized, no NAT Gateway)"
    );
    debug!(
        "⚙️ EC2 config: ARM64 instances={:?}, min/max vCPUs={}/{}, allocation={}",
        config.yaml_config.batch.ec2_config.instance_types,
        config.yaml_config.batch.ec2_config.min_vcpus,
        config.yaml_config.batch.ec2_config.max_vcpus,
        config.yaml_config.batch.ec2_config.allocation_strategy
    );

    info!("🔧 Creating EC2 MANAGED compute environment with ARM64 Graviton instances");
    debug!("🔒 Using public subnet for cost-optimized Docker builds (no NAT Gateway needed)");
    debug!("🛡️ Security group configured for EC2 batch workloads");
    debug!("💰 Cost optimization: ARM64 Graviton instances provide 20% better price-performance");
    debug!("🔧 Creating EC2 compute environment with VPC dependencies");
    debug!("   - Using proper Pulumi Gestalt InputOrOutput pattern");
    debug!("   - Reactive outputs convert to InputOrOutput automatically via From trait");
    debug!("   - Static values in nested objects, reactive outputs for top-level fields");

    // Extract config values to avoid lifetime issues in closure
    let min_vcpus = config.yaml_config.batch.ec2_config.min_vcpus as i32;
    let max_vcpus = config.yaml_config.batch.ec2_config.max_vcpus as i32;
    let desired_vcpus = config.yaml_config.batch.ec2_config.desired_vcpus as i32;
    let instance_types = config.yaml_config.batch.ec2_config.instance_types.clone();
    let allocation_strategy = config.yaml_config.batch.ec2_config.allocation_strategy.clone();

    // Use pulumi_combine! following Pulumi Gestalt library patterns
    // Output<T> auto-converts to InputOrOutput<T> via From trait implementation
    let compute_resources_output = pulumi_combine!(
        vpc_infra.public_subnet.id.clone(),
        vpc_infra.batch_security_group.id.clone(),
        instance_profile.arn.clone(),
        launch_template.name.clone()
    ).map(move |(subnet_id, security_group_id, instance_role_arn, launch_template_name)| {
        debug!("🔄 Resolving VPC dependencies: subnet_id, security_group_id, instance_role_arn, launch_template");
        crate::aws::types::batch::ComputeEnvironmentComputeResources::builder()
            .type_("EC2".to_string())
            .instance_types(instance_types.clone())  // Optional: x86_64 instances from config
            .subnets(vec![subnet_id])  // Required: Static String values in nested object
            .security_group_ids(vec![security_group_id])  // Optional: Static String values
            .instance_role(instance_role_arn)  // Optional: Required for EC2 instances
            .min_vcpus(min_vcpus)  // Optional: Scaling parameters from config
            .max_vcpus(max_vcpus)  // Required: No Some() needed
            .desired_vcpus(desired_vcpus)  // Optional: Scaling parameters from config
            .allocation_strategy(allocation_strategy.clone())  // Optional: x86_64 allocation strategy
            .launch_template(
                crate::aws::types::batch::ComputeEnvironmentComputeResourcesLaunchTemplate::builder()
                    .launch_template_name(launch_template_name)
                    .version("$Latest")
                    .build_struct()
            )  // Launch template with larger EBS volume
            .build_struct()
    });

    let compute_environment = crate::aws::batch::compute_environment::create(
        context,
        "ec2-compute-env-vpc",
        crate::aws::batch::compute_environment::ComputeEnvironmentArgs::builder()
            .name(format!("{}-ec2-vpc", config.yaml_config.batch.compute_environment_name))
            .type_("MANAGED".to_string())
            .state("ENABLED".to_string())
            // .service_role(service_role.arn.clone())  // COMMENTED OUT: Using empty string for service-linked role
            .service_role("".to_string())  // Empty string tells AWS to use service-linked role
            .compute_resources(compute_resources_output)  // Output<ComputeEnvironmentComputeResources>
            .tags({
                let mut env_tags = tags.clone();
                env_tags.insert("ComputeType".to_string(), "EC2".to_string());
                env_tags.insert("Architecture".to_string(), "ARM64".to_string());
                env_tags
            })
            .build_struct(),
    );

    info!(
        "✅ EC2 MANAGED compute environment created: {}-ec2-vpc",
        config.yaml_config.batch.compute_environment_name
    );
    debug!("📋 EC2 VPC integration summary:");
    debug!("  - Subnet: Public subnet (cost-optimized, no NAT Gateway)");
    debug!("  - Security Group: EC2 batch-specific rules");
    debug!("  - Instance Profile: Full ECR and S3 access");
    debug!("  - Service Role: Batch service management");
    debug!("  - ARM64 Instances: C6G, M6G families (explicitly supported)");
    debug!("  - Cost Savings: ~$45/month from eliminating NAT Gateway + ARM64 optimization");

    Ok(compute_environment)
}

/// Creates AWS Batch Fargate MANAGED compute environment with VPC integration
fn create_fargate_managed_compute_environment_with_vpc(
    context: &Context,
    config: &Config,
    _vpc_infra: &VpcInfrastructure,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::batch::compute_environment::ComputeEnvironmentResult> {
    info!("💻 Creating Fargate MANAGED compute environment");
    debug!(
        "🌐 VPC config: using public subnet for Fargate tasks (cost-optimized, no NAT Gateway)"
    );
    debug!(
        "⚙️ Fargate config: platform={}, max vCPUs={}",
        config.yaml_config.batch.fargate_config.platform_version,
        config.yaml_config.batch.fargate_config.max_vcpus
    );


    info!("🔧 Creating Fargate MANAGED compute environment");
    debug!("🔒 Using public subnet for cost-optimized Docker builds (no NAT Gateway needed)");
    debug!("🛡️ Security group configured for Fargate workloads");
    debug!("💰 Cost optimization: Fargate serverless billing model");
    debug!("🔧 Creating Fargate compute environment with VPC dependencies");
    debug!("   - Using proper Pulumi Gestalt InputOrOutput pattern");
    debug!("   - Reactive outputs convert to InputOrOutput automatically via From trait");
    debug!("   - Static values in nested objects, reactive outputs for top-level fields");

    // Extract config value to avoid lifetime issues in closure
    let max_vcpus = config.yaml_config.batch.fargate_config.max_vcpus as i32;

    // Use pulumi_combine! following Pulumi Gestalt library patterns
    // Output<T> auto-converts to InputOrOutput<T> via From trait implementation
    let compute_resources_output = pulumi_combine!(
        _vpc_infra.public_subnet.id.clone(),
        _vpc_infra.batch_security_group.id.clone()
    ).map(move |(subnet_id, security_group_id)| {
        debug!("🔄 Resolving VPC dependencies: subnet_id and security_group_id");
        crate::aws::types::batch::ComputeEnvironmentComputeResources::builder()
            .type_("FARGATE".to_string())
            .subnets(vec![subnet_id])  // Static String values in nested object
            .security_group_ids(vec![security_group_id])  // Static String values
            .max_vcpus(max_vcpus)  // No config reference in closure
            .build_struct()
    });

    let compute_environment = crate::aws::batch::compute_environment::create(
        context,
        "fargate-compute-env-vpc",
        crate::aws::batch::compute_environment::ComputeEnvironmentArgs::builder()
            .name(format!("{}-fargate-vpc", config.yaml_config.batch.compute_environment_name))
            .type_("MANAGED".to_string())
            .state("ENABLED".to_string())
            // .service_role(service_role.arn.clone())  // COMMENTED OUT: Using empty string for service-linked role
            .service_role("".to_string())  // Empty string tells AWS to use service-linked role
            .compute_resources(compute_resources_output)  // Output<ComputeEnvironmentComputeResources>
            .tags({
                let mut env_tags = tags.clone();
                env_tags.insert("ComputeType".to_string(), "FARGATE".to_string());
                env_tags
            })
            .build_struct(),
    );

    info!(
        "✅ Fargate MANAGED compute environment created: {}-fargate-vpc",
        config.yaml_config.batch.compute_environment_name
    );
    debug!("📋 Fargate VPC integration summary:");
    debug!("  - Subnet: Public subnet (cost-optimized, no NAT Gateway)");
    debug!("  - Security Group: Fargate-specific rules");
    debug!("  - Platform Version: {}", config.yaml_config.batch.fargate_config.platform_version);
    debug!("  - Service Role: Batch service management");
    debug!("  - Cost Savings: ~$45/month from eliminating NAT Gateway + serverless pricing");

    Ok(compute_environment)
}

/// Creates AWS Batch job queue
fn create_job_queue(
    context: &Context,
    config: &Config,
    compute_environment: &crate::aws::batch::compute_environment::ComputeEnvironmentResult,
    _tags: &HashMap<String, String>,
) -> Result<crate::aws::batch::job_queue::JobQueueResult> {
    info!("📋 Creating Batch job queue with proper compute environment orders");
    debug!(
        "🔄 Job queue target: {} (MANAGED compute environment)",
        config.yaml_config.batch.compute_environment_name
    );
    debug!(
        "📋 Job queue config: priority=1 (highest), state=ENABLED, with compute environment reference"
    );

    // Create compute environment order configuration
    info!("🔧 Creating minimal job queue with only required fields");
    debug!("🔗 Using proper Pulumi Gestalt pattern for JobQueue compute environment orders");
    debug!("   - Creating reactive Output<Vec<JobQueueComputeEnvironmentOrder>>");
    debug!("   - Static values in nested object, reactive output for top-level field");

    // Create reactive output for compute environment orders following Pulumi Gestalt patterns
    // Map the compute environment ARN to static nested object
    let compute_env_orders_output = compute_environment.arn.map(|arn| {
        vec![crate::aws::types::batch::JobQueueComputeEnvironmentOrder::builder()
            .order(1i32)
            .compute_environment(arn)  // Static String value in nested object
            .build_struct()]
    });

    // Create dynamic job queue name that includes compute environment reference
    // This forces job queue replacement when compute environment changes
    let job_queue_name_output = compute_environment.name.map(|ce_name| {
        format!("queue-{}", ce_name.replace("docker-build-environment-", ""))
    });

    let job_queue = crate::aws::batch::job_queue::create(
        context,
        "job-queue",
        crate::aws::batch::job_queue::JobQueueArgs::builder()
            .name(job_queue_name_output)  // Dynamic name forces replacement
            .state("ENABLED".to_string())
            .priority(1i32)
            .compute_environment_orders(compute_env_orders_output)  // Output<Vec<T>> auto-converts to InputOrOutput
            .build_struct(),
    );

    info!(
        "✅ Dynamic job queue created with compute environment dependency (forces replacement)"
    );
    Ok(job_queue)
}

/// Creates AWS Batch job definition for Docker image building (supports both EC2 and Fargate)
fn create_job_definition(
    context: &Context,
    config: &Config,
    execution_role: &crate::aws::iam::role::RoleResult,
    ecr_repository: &crate::aws::ecrpublic::repository::RepositoryResult,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::batch::job_definition::JobDefinitionResult> {
    info!("📝 Creating Batch job definition with platform-specific container configuration");
    debug!(
        "🐳 Container config: image={}, vCPUs={}, memory={}MB, timeout={}s, platform={}",
        config.yaml_config.batch.container_image,
        config.yaml_config.batch.vcpus,
        config.yaml_config.batch.memory,
        config.yaml_config.batch.job_timeout_seconds,
        config.yaml_config.batch.compute_environment_type
    );
    debug!(
        "✅ Following container best practices: platform capabilities, environment variables"
    );

    // Container properties with reactive execution role ARN and ECR repository URI
    let container_image = config.yaml_config.batch.container_image.clone();
    let vcpus_str = config.yaml_config.batch.vcpus.to_string();
    let memory = config.yaml_config.batch.memory;
    let region = config.yaml_config.batch.region.clone();
    let job_queue_name = config.yaml_config.batch.job_queue_name.clone();
    let s3_region = config.yaml_config.s3.region.clone();
    let s3_bucket_name = config.yaml_config.s3.bucket_name.clone();
    let compute_env_type = config.yaml_config.batch.compute_environment_type.clone();

    let container_properties_output = pulumi_combine!(
        execution_role.arn.clone(),
        ecr_repository.repository_uri.clone()
    ).map(move |(execution_arn, ecr_repository_uri)| {
        // Build ulimits section conditionally (Fargate doesn't support ulimits)
        let ulimits_section = if compute_env_type == "fargate" {
            "" // No ulimits for Fargate
        } else {
            r#",
            "ulimits": [
                {
                    "name": "nofile",
                    "softLimit": 65536,
                    "hardLimit": 65536
                }
            ]"#
        };

        // Build volumes and mountPoints sections conditionally (Fargate doesn't support Docker socket mounting)
        let (mount_points_section, volumes_section) = if compute_env_type == "fargate" {
            ("", "") // No Docker socket for Fargate
        } else {
            (
                r#",
            "mountPoints": [
                {
                    "sourceVolume": "docker-socket",
                    "containerPath": "/var/run/docker.sock",
                    "readOnly": false
                }
            ]"#,
                r#",
            "volumes": [
                {
                    "name": "docker-socket",
                    "host": {
                        "sourcePath": "/var/run/docker.sock"
                    }
                }
            ]"#,
            )
        };

        // Build privileged setting conditionally (Fargate requires false)
        let privileged_setting = if compute_env_type == "fargate" {
            "false"
        } else {
            "true" // EC2 can use privileged mode for Docker socket access
        };

        // Build network configuration section conditionally (Fargate needs public IP for Docker Hub access)
        let network_config_section = if compute_env_type == "fargate" {
            r#",
            "networkConfiguration": {
                "assignPublicIp": "ENABLED"
            }"#
        } else {
            "" // No network configuration needed for EC2
        };

        format!(
            r#"{{
            "image": "{}",
            "command": [
                "/bin/sh",
                "-c",
                "apk add --no-cache aws-cli unzip && aws s3 cp s3://$S3_BUCKET_NAME/source.zip . && unzip source.zip && aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws && COMMIT_HASH=$(date +%s | cut -c1-7) && docker build -f docker/Dockerfile -t $ECR_REPOSITORY:latest . --progress=plain && docker tag $ECR_REPOSITORY:latest $ECR_REPOSITORY:$COMMIT_HASH && docker push $ECR_REPOSITORY:latest && docker push $ECR_REPOSITORY:$COMMIT_HASH && echo 'ComfyUI Docker image built and pushed successfully'"
            ],
            "resourceRequirements": [
                {{"type": "VCPU", "value": "{}"}},
                {{"type": "MEMORY", "value": "{}"}}
            ],
            "jobRoleArn": "{}",
            "executionRoleArn": "{}",
            "readonlyRootFilesystem": false,
            "privileged": {},
            "user": "root",
            "environment": [
                {{"name": "AWS_DEFAULT_REGION", "value": "{}"}},
                {{"name": "ECR_REPOSITORY", "value": "{}"}},
                {{"name": "ECR_REGION", "value": "us-east-1"}},
                {{"name": "DOCKER_BUILDKIT", "value": "1"}},
                {{"name": "BUILDKIT_PROGRESS", "value": "plain"}},
                {{"name": "BATCH_JOB_QUEUE", "value": "{}"}},
                {{"name": "S3_BUCKET_REGION", "value": "{}"}},
                {{"name": "S3_BUCKET_NAME", "value": "{}"}}
            ],
            "logConfiguration": {{
                "logDriver": "awslogs",
                "options": {{
                    "awslogs-group": "/aws/batch/container-builds",
                    "awslogs-region": "{}",
                    "awslogs-stream-prefix": "batch-job"
                }}
            }}{}{}{}{}
        }}"#,
            container_image,
            vcpus_str,
            memory,
            execution_arn, // Execution role ARN for jobRoleArn (AWS service access)
            execution_arn, // Execution role ARN for executionRoleArn (container lifecycle)
            privileged_setting, // Conditionally set privileged mode
            region,
            ecr_repository_uri,
            job_queue_name,
            s3_region,
            s3_bucket_name,
            region, // for awslogs-region
            mount_points_section, // Conditionally include mountPoints
            volumes_section, // Conditionally include volumes
            ulimits_section, // Conditionally include ulimits
            network_config_section // Conditionally include networkConfiguration for Fargate
        )
    });

    // Enhanced job definition with platform-specific configuration
    let (platform_capabilities, job_tags) = match config.yaml_config.batch.compute_environment_type.as_str() {
        "ec2" => {
            debug!("⚙️ Job definition: container type with EC2 platform capability");
            debug!("🔧 Container features: Docker socket mount, privileged mode, ulimits configured");
            let mut tags = tags.clone();
            tags.insert("JobType".to_string(), "DockerBuild".to_string());
            tags.insert("Platform".to_string(), "EC2".to_string());
            tags.insert("Architecture".to_string(), "ARM64".to_string());
            (vec!["EC2".to_string()], tags)
        },
        "fargate" => {
            debug!("⚙️ Job definition: container type with Fargate platform capability");
            debug!("🔧 Container features: Fargate serverless execution environment");
            let mut tags = tags.clone();
            tags.insert("JobType".to_string(), "DockerBuild".to_string());
            tags.insert("Platform".to_string(), "FARGATE".to_string());
            (vec!["FARGATE".to_string()], tags)
        },
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported compute environment type for job definition: {}",
                config.yaml_config.batch.compute_environment_type
            ));
        }
    };

    let job_definition = crate::aws::batch::job_definition::create(
        context,
        "job-definition",
        crate::aws::batch::job_definition::JobDefinitionArgs::builder()
            .name(config.yaml_config.batch.job_definition_name.clone())
            .type_("container".to_string())
            .platform_capabilities(platform_capabilities)
            .container_properties(container_properties_output)
            .tags(job_tags)
            .build_struct(),
    );

    info!(
        "✅ Job definition created: {}",
        config.yaml_config.batch.job_definition_name
    );
    debug!("📊 Enhanced job definition summary:");
    debug!(
        "  - Container: {} ({})",
        config.yaml_config.batch.container_image,
        config.yaml_config.batch.compute_environment_type
    );
    debug!(
        "  - Resources: {}vCPU, {}MB memory",
        config.yaml_config.batch.vcpus, config.yaml_config.batch.memory
    );
    debug!("  - Platform: {} with optimized configuration", config.yaml_config.batch.compute_environment_type.to_uppercase());
    debug!("  - Environment: AWS regions, ECR repository, build tools configured");
    match config.yaml_config.batch.compute_environment_type.as_str() {
        "ec2" => {
            debug!("  - EC2 Features: Docker socket mount, privileged mode, ulimits configured");
            debug!("  - ARM64 Optimization: Graviton2/3 instances for cost efficiency");
        },
        "fargate" => {
            debug!("  - Fargate Features: Serverless execution, managed scaling");
            debug!("  - Cost Model: Pay-per-use, no idle capacity charges");
        },
        _ => {}
    }

    Ok(job_definition)
}
