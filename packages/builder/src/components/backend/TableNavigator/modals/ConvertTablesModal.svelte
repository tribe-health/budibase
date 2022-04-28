<script>
  import { datasources } from "stores/backend"
  import { notifications } from "@budibase/bbui"
  import { Select, Body, ModalContent } from "@budibase/bbui"
  import { onMount } from "svelte"

  let datasourceId
  $: dsList = $datasources.list

  async function convertTables() {
    try {
      const tableIds = await datasources.convert(datasourceId)
      notifications.success(
        `Conversion completed, converted ${tableIds.length} tables`
      )
    } catch (err) {
      notifications.error(
        `Failed to convert, reason: ${err?.message ? err.message : err}`
      )
    }
  }

  onMount(async () => {
    await datasources.fetch()
  })
</script>

<ModalContent
  title="Convert tables"
  confirmText="Convert"
  onConfirm={convertTables}
  disabled={!datasourceId}
>
  <Body>Select a datasource to convert your tables to.</Body>
  <Select
    label="Datasource"
    bind:value={datasourceId}
    options={dsList.filter(ds => ds.source !== "BUDIBASE")}
    getOptionValue={ds => ds._id}
    getOptionLabel={ds => ds.name}
  />
</ModalContent>
