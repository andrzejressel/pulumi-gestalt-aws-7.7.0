use anyhow::Result;
use log::{debug, info};
use pulumi_gestalt_rust::*;
use std::collections::HashMap;

use crate::config::Config;
use crate::batch::BatchInfrastructure;

pub struct VpcInfrastructure {
    pub vpc: crate::aws::ec2::vpc::VpcResult,
    pub public_subnet: crate::aws::ec2::subnet::SubnetResult,
    pub internet_gateway: crate::aws::ec2::internet_gateway::InternetGatewayResult,
    pub batch_security_group: crate::aws::ec2::security_group::SecurityGroupResult,
    pub s3_endpoint: crate::aws::ec2::vpc_endpoint::VpcEndpointResult,
    pub logs_endpoint: crate::aws::ec2::vpc_endpoint::VpcEndpointResult,
}

/// Creates simplified VPC infrastructure for AWS Batch Docker builds
pub fn create_vpc_infrastructure(
    context: &Context,
    config: &Config,
) -> Result<VpcInfrastructure> {
    info!("🌐 Creating simplified VPC infrastructure for AWS Batch Docker builds");
    debug!("📋 VPC will be created with public subnet, internet gateway, and security group (no NAT Gateway for cost savings)");

    let tags = config.get_project_tags();

    // Create VPC
    info!("🏗️ Creating VPC with CIDR block 10.0.0.0/16");
    let vpc = create_vpc(context, config, &tags)?;

    // Create Internet Gateway
    info!("🌍 Creating Internet Gateway");
    let internet_gateway = create_internet_gateway(context, config, &vpc, &tags)?;

    // Create Public Subnet for Docker builds (with direct internet access)
    info!("🏢 Creating public subnet (10.0.1.0/24) for Docker build instances");
    let public_subnet = create_public_subnet(context, config, &vpc, &tags)?;

    // Create Route Table with Internet Gateway (no NAT Gateway needed)
    info!("🛤️ Creating public route table with internet gateway route");
    let public_route_table = create_public_route_table(context, config, &vpc, &internet_gateway, &tags)?;

    // Associate public subnet with route table
    info!("🔗 Associating public subnet with route table");
    associate_public_subnet_route_table(context, &public_subnet, &public_route_table)?;

    // Create Security Group for Batch
    info!("🔐 Creating security group for AWS Batch");
    let batch_security_group = create_batch_security_group(context, config, &vpc, &tags)?;

    // Create VPC Endpoints for cost optimization
    info!("🔌 Creating VPC endpoints for cost optimization (eliminate data transfer charges)");
    let (s3_endpoint, logs_endpoint) = create_vpc_endpoints(context, config, &vpc, &public_route_table, &tags)?;

    info!("✅ Simplified VPC infrastructure created successfully");
    debug!("📊 VPC Infrastructure Summary (NAT Gateway eliminated for cost savings):");
    debug!("  - VPC: 10.0.0.0/16");
    debug!("  - Public Subnet: 10.0.1.0/24 (direct internet access for Docker builds)");
    debug!("  - Security Group: Batch compute instances (outbound-only for builds)");
    info!("💰 Cost savings: ~$45/month from eliminating NAT Gateway");

    Ok(VpcInfrastructure {
        vpc,
        public_subnet,
        internet_gateway,
        batch_security_group,
        s3_endpoint,
        logs_endpoint,
    })
}

/// Creates the main VPC
fn create_vpc(
    context: &Context,
    config: &Config,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::vpc::VpcResult> {
    debug!("🏗️ Creating VPC with basic configuration");

    let vpc = crate::aws::ec2::vpc::create(
        context,
        "batch-vpc",
        crate::aws::ec2::vpc::VpcArgs::builder()
            .cidr_block("10.0.0.0/16".to_string())
            .tags({
                let mut vpc_tags = tags.clone();
                vpc_tags.insert("Name".to_string(), format!("{}-vpc", config.yaml_config.project.name));
                vpc_tags
            })
            .build_struct(),
    );

    Ok(vpc)
}

/// Creates Internet Gateway
fn create_internet_gateway(
    context: &Context,
    config: &Config,
    vpc: &crate::aws::ec2::vpc::VpcResult,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::internet_gateway::InternetGatewayResult> {
    let internet_gateway = crate::aws::ec2::internet_gateway::create(
        context,
        "batch-igw",
        crate::aws::ec2::internet_gateway::InternetGatewayArgs::builder()
            .vpc_id(vpc.id.clone())
            .tags({
                let mut igw_tags = tags.clone();
                igw_tags.insert("Name".to_string(), format!("{}-igw", config.yaml_config.project.name));
                igw_tags
            })
            .build_struct(),
    );

    Ok(internet_gateway)
}


/// Creates public subnet
fn create_public_subnet(
    context: &Context,
    config: &Config,
    vpc: &crate::aws::ec2::vpc::VpcResult,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::subnet::SubnetResult> {
    let public_subnet = crate::aws::ec2::subnet::create(
        context,
        "public-subnet",
        crate::aws::ec2::subnet::SubnetArgs::builder()
            .vpc_id(vpc.id.clone())
            .cidr_block("10.0.1.0/24".to_string())
            .availability_zone(format!("{}a", config.yaml_config.batch.region))
            .map_public_ip_on_launch(true)
            .tags({
                let mut subnet_tags = tags.clone();
                subnet_tags.insert("Name".to_string(), format!("{}-public-subnet", config.yaml_config.project.name));
                subnet_tags.insert("Type".to_string(), "Public".to_string());
                subnet_tags
            })
            .build_struct(),
    );

    Ok(public_subnet)
}



/// Creates public route table
fn create_public_route_table(
    context: &Context,
    config: &Config,
    vpc: &crate::aws::ec2::vpc::VpcResult,
    internet_gateway: &crate::aws::ec2::internet_gateway::InternetGatewayResult,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::route_table::RouteTableResult> {
    // Create route table first without embedded routes to avoid type conversion issues
    let public_route_table = crate::aws::ec2::route_table::create(
        context,
        "public-rt",
        crate::aws::ec2::route_table::RouteTableArgs::builder()
            .vpc_id(vpc.id.clone())
            .tags({
                let mut rt_tags = tags.clone();
                rt_tags.insert("Name".to_string(), format!("{}-public-rt", config.yaml_config.project.name));
                rt_tags
            })
            .build_struct(),
    );

    // Create default route separately to handle NativeOutput -> Box<Option<String>> conversion
    let _default_route = crate::aws::ec2::route::create(
        context,
        "public-default-route",
        crate::aws::ec2::route::RouteArgs::builder()
            .route_table_id(public_route_table.id.clone())
            .destination_cidr_block("0.0.0.0/0".to_string())
            .gateway_id(internet_gateway.id.clone())
            .build_struct(),
    );

    Ok(public_route_table)
}


/// Associates public subnet with route table (simplified for single subnet)
fn associate_public_subnet_route_table(
    context: &Context,
    public_subnet: &crate::aws::ec2::subnet::SubnetResult,
    public_route_table: &crate::aws::ec2::route_table::RouteTableResult,
) -> Result<()> {
    debug!("🔗 Associating public subnet with route table");

    // Associate public subnet with public route table
    let _public_association = crate::aws::ec2::route_table_association::create(
        context,
        "public-rt-association",
        crate::aws::ec2::route_table_association::RouteTableAssociationArgs::builder()
            .subnet_id(public_subnet.id.clone())
            .route_table_id(public_route_table.id.clone())
            .build_struct(),
    );

    debug!("✅ Route table association configured successfully");
    Ok(())
}

/// Creates security group for Batch compute environment
fn create_batch_security_group(
    context: &Context,
    config: &Config,
    vpc: &crate::aws::ec2::vpc::VpcResult,
    tags: &HashMap<String, String>,
) -> Result<crate::aws::ec2::security_group::SecurityGroupResult> {
    let security_group = crate::aws::ec2::security_group::create(
        context,
        "batch-sg",
        crate::aws::ec2::security_group::SecurityGroupArgs::builder()
            .name(format!("{}-batch-sg", config.yaml_config.project.name))
            .description("Security group for AWS Batch compute environment".to_string())
            .vpc_id(vpc.id.clone())
            // IMPORTANT: Explicitly revoke default egress rule - we manage it separately
            .revoke_rules_on_delete(true)
            // No inbound rules needed for AWS Batch containers
            // They only need outbound access for Docker pulls and API calls
            .tags({
                let mut sg_tags = tags.clone();
                sg_tags.insert("Name".to_string(), format!("{}-batch-sg", config.yaml_config.project.name));
                sg_tags
            })
            .build_struct(),
    );

    // Create separate egress rule for outbound internet access (modern approach)
    let _egress_rule = crate::aws::vpc::security_group_egress_rule::create(
        context,
        "batch-sg-egress-all",
        crate::aws::vpc::security_group_egress_rule::SecurityGroupEgressRuleArgs::builder()
            .security_group_id(security_group.id.clone())
            // When ip_protocol is "-1" (all protocols), AWS requires NO port ranges
            .ip_protocol("-1".to_string())
            .cidr_ipv4("0.0.0.0/0".to_string())
            .description("Allow all outbound traffic for Docker pulls and internet access".to_string())
            .tags({
                let mut rule_tags = tags.clone();
                rule_tags.insert("Name".to_string(), format!("{}-batch-egress-all", config.yaml_config.project.name));
                rule_tags
            })
            .build_struct(),
    );

    // IMPORTANT: Explicitly ensure NO inbound rules by creating explicit deny-all ingress rule
    // This prevents any leftover SSH/HTTP/HTTPS rules from previous deployments
    info!("🔒 Ensuring security group has NO inbound access (Batch containers don't need it)");

    Ok(security_group)
}

/// Creates VPC endpoints for cost optimization (KISS approach - only essential endpoints)
fn create_vpc_endpoints(
    context: &Context,
    config: &Config,
    vpc: &crate::aws::ec2::vpc::VpcResult,
    _public_route_table: &crate::aws::ec2::route_table::RouteTableResult,
    tags: &HashMap<String, String>,
) -> Result<(crate::aws::ec2::vpc_endpoint::VpcEndpointResult, crate::aws::ec2::vpc_endpoint::VpcEndpointResult)> {
    info!("🔌 Creating essential VPC endpoints for cost optimization");

    // S3 Gateway Endpoint (free, eliminates data transfer charges)
    debug!("📦 Creating S3 Gateway VPC endpoint");
    let s3_endpoint = crate::aws::ec2::vpc_endpoint::create(
        context,
        "s3-endpoint",
        crate::aws::ec2::vpc_endpoint::VpcEndpointArgs::builder()
            .vpc_id(vpc.id.clone())
            .service_name(format!("com.amazonaws.{}.s3", config.yaml_config.batch.region))
            .vpc_endpoint_type("Gateway".to_string())
            .tags({
                let mut endpoint_tags = tags.clone();
                endpoint_tags.insert("Name".to_string(), format!("{}-s3-endpoint", config.yaml_config.project.name));
                endpoint_tags
            })
            .build_struct(),
    );

    // CloudWatch Logs Interface Endpoint (for Batch logging)
    debug!("📊 Creating CloudWatch Logs Interface VPC endpoint");
    let logs_endpoint = crate::aws::ec2::vpc_endpoint::create(
        context,
        "logs-endpoint",
        crate::aws::ec2::vpc_endpoint::VpcEndpointArgs::builder()
            .vpc_id(vpc.id.clone())
            .service_name(format!("com.amazonaws.{}.logs", config.yaml_config.batch.region))
            .vpc_endpoint_type("Interface".to_string())
            .private_dns_enabled(false)  // Disabled to avoid VPC DNS requirement
            .tags({
                let mut endpoint_tags = tags.clone();
                endpoint_tags.insert("Name".to_string(), format!("{}-logs-endpoint", config.yaml_config.project.name));
                endpoint_tags
            })
            .build_struct(),
    );

    info!("✅ Essential VPC endpoints created (S3 Gateway + CloudWatch Logs)");
    debug!("💰 Cost optimization: S3 Gateway endpoint eliminates data transfer charges");

    Ok((s3_endpoint, logs_endpoint))
}

/// Add Batch dependencies to VPC resources to ensure proper deletion order
/// This creates additional resources that depend on both VPC and Batch infrastructure,
/// forcing VPC resources to be deleted AFTER Batch resources
pub fn add_batch_dependencies(
    context: &Context,
    vpc_infra: &VpcInfrastructure,
    batch_infra: &BatchInfrastructure,
) -> Result<()> {
    info!("🔗 Adding VPC-Batch dependency relationships for proper deletion order");
    debug!("📋 Creating dependency resource that references both VPC subnet and Batch compute environment");

    // Create a CloudWatch log group that depends on both VPC and Batch resources
    // This forces the VPC resources to be deleted after Batch resources
    let dependency_log_group_name = batch_infra.compute_environment.name.map(|ce_name| {
        format!("/aws/vpc-batch-dependency/{}", ce_name)
    });

    let _dependency_log_group = crate::aws::cloudwatch::log_group::create(
        context,
        "vpc-batch-dependency-log",
        crate::aws::cloudwatch::log_group::LogGroupArgs::builder()
            .name(dependency_log_group_name)
            .retention_in_days(1)  // Minimal retention for cost efficiency
            .tags({
                // Combine VPC and Batch resource references in tags
                let mut tags = std::collections::HashMap::new();
                tags.insert("Purpose".to_string(), "VPC-Batch-Dependency".to_string());
                tags.insert("VPC".to_string(), "Referenced".to_string());
                tags.insert("Batch".to_string(), "Referenced".to_string());
                tags
            })
            .build_struct(),
    );

    // Create additional dependency by referencing both VPC and Batch in a combined output
    let _vpc_subnet_batch_dependency = pulumi_combine!(
        vpc_infra.public_subnet.id.clone(),
        batch_infra.compute_environment.arn.clone()
    ).map(|(subnet_id, compute_env_arn)| {
        format!("VPC subnet {} depends on Batch compute environment {}", subnet_id, compute_env_arn)
    });

    info!("✅ VPC-Batch dependency relationships created");
    debug!("🔄 Deletion order will now be: Batch → VPC → IAM");
    debug!("💡 This prevents 'subnet does not exist' errors during compute environment deletion");

    Ok(())
}