use catalyst_operator::api::crds;

fn main() -> anyhow::Result<()> {
    for crd in crds() {
        print!(
            "---\n{}",
            serde_yaml::to_string(&crd)?
        );
    }

    Ok(())
}
