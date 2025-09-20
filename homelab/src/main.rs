use anyhow::Result;
use pulumi_aws::ec2::instance;
use pulumi_aws::ec2::instance::InstanceArgs;
use pulumi_gestalt_rust::*;

pulumi_main!();

fn pulumi_main(context: &Context) -> Result<()> {
    instance::create(context, "test", InstanceArgs::builder().build_struct());

    Ok(())
}
