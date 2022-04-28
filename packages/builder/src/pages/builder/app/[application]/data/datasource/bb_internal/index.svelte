<script>
  import { Button, Heading, Body, Layout, Modal, Divider } from "@budibase/bbui"
  import CreateTableModal from "components/backend/TableNavigator/modals/CreateTableModal.svelte"
  import ConvertTablesModal from "components/backend/TableNavigator/modals/ConvertTablesModal.svelte"
  import ICONS from "components/backend/DatasourceNavigator/icons"
  import { tables } from "stores/backend"
  import { goto } from "@roxi/routify"

  let createTableModal, convertTablesModal
</script>

<Modal bind:this={createTableModal}>
  <CreateTableModal />
</Modal>
<Modal bind:this={convertTablesModal}>
  <ConvertTablesModal />
</Modal>

<section>
  <Layout>
    <Layout gap="XS" noPadding>
      <header>
        <svelte:component this={ICONS.BUDIBASE} height="26" width="26" />
        <Heading size="M">Budibase Internal</Heading>
      </header>
      <Body size="M">
        Budibase internal tables are part of your app, so the data will be
        stored in your apps context.
      </Body>
    </Layout>
    <Divider />
    <Heading size="S">Tables</Heading>
    <div class="table-list">
      {#each $tables.list.filter(table => table.type !== "external") as table}
        <div
          class="table-list-item"
          on:click={$goto(`../../table/${table._id}`)}
        >
          <Body size="S">{table.name}</Body>
          {#if table.primaryDisplay}
            <Body size="S">Display column: {table.primaryDisplay}</Body>
          {/if}
        </div>
      {/each}
    </div>
    <div class="buttons">
      <Button cta on:click={createTableModal.show}>Create new table</Button>
      <Button secondary on:click={convertTablesModal.show}
        >Convert tables</Button
      >
    </div>
  </Layout>
</section>

<style>
  section {
    margin: 0 auto;
    width: 640px;
  }

  header {
    display: flex;
    gap: var(--spacing-l);
    align-items: center;
  }

  .table-list {
    display: flex;
    flex-direction: column;
    gap: var(--spacing-m);
  }

  .table-list-item {
    border-radius: var(--border-radius-m);
    background: var(--background);
    border: var(--border-dark);
    display: grid;
    grid-template-columns: 2fr 0.75fr 20px;
    align-items: center;
    padding: var(--spacing-m);
    gap: var(--layout-xs);
    transition: 200ms background ease;
  }

  .table-list-item:hover {
    background: var(--grey-1);
    cursor: pointer;
  }

  .buttons {
    display: flex;
    flex-direction: row;
    gap: var(--spacing-l);
  }
</style>
