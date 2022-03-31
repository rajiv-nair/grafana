import { css } from '@emotion/css';
import { DataSourceApi, GrafanaTheme2 } from '@grafana/data';
import { Stack } from '@grafana/experimental';
import { Button, Cascader, CascaderOption, useStyles2 } from '@grafana/ui';
import React, { useState } from 'react';
import { DragDropContext, Droppable, DropResult } from 'react-beautiful-dnd';
import { QueryBuilderOperation, QueryWithOperations, VisualQueryModeller } from '../shared/types';
import { OperationEditor } from './OperationEditor';

export interface Props<T extends QueryWithOperations> {
  query: T;
  datasource: DataSourceApi;
  onChange: (query: T) => void;
  onRunQuery: () => void;
  queryModeller: VisualQueryModeller;
  explainMode?: boolean;
}

export function OperationList<T extends QueryWithOperations>({
  query,
  datasource,
  queryModeller,
  onChange,
  onRunQuery,
}: Props<T>) {
  const styles = useStyles2(getStyles);
  const { operations } = query;

  const [cascaderOpen, setCascaderOpen] = useState(false);

  const onOperationChange = (index: number, update: QueryBuilderOperation) => {
    const updatedList = [...operations];
    updatedList.splice(index, 1, update);
    onChange({ ...query, operations: updatedList });
  };

  const onRemove = (index: number) => {
    const updatedList = [...operations.slice(0, index), ...operations.slice(index + 1)];
    onChange({ ...query, operations: updatedList });
  };

  const addOptions: CascaderOption[] = queryModeller.getCategories().map((category) => {
    return {
      value: category,
      label: category,
      items: queryModeller.getOperationsForCategory(category).map((operation) => ({
        value: operation.id,
        label: operation.name,
        isLeaf: true,
      })),
    };
  });

  const onAddOperation = (value: string) => {
    const operationDef = queryModeller.getOperationDef(value);
    if (!operationDef) {
      return;
    }
    onChange(operationDef.addOperationHandler(operationDef, query, queryModeller));
    setCascaderOpen(false);
  };

  const onDragEnd = (result: DropResult) => {
    if (!result.destination) {
      return;
    }

    const updatedList = [...operations];
    const element = updatedList[result.source.index];
    updatedList.splice(result.source.index, 1);
    updatedList.splice(result.destination.index, 0, element);
    onChange({ ...query, operations: updatedList });
  };

  const onCascaderBlur = () => {
    setCascaderOpen(false);
  };

  return (
    <Stack gap={1} direction="column">
      <Stack gap={1}>
        {operations.length > 0 && (
          <DragDropContext onDragEnd={onDragEnd}>
            <Droppable droppableId="sortable-field-mappings" direction="horizontal">
              {(provided) => (
                <div className={styles.operationList} ref={provided.innerRef} {...provided.droppableProps}>
                  {operations.map((op, index) => (
                    <OperationEditor
                      key={index}
                      queryModeller={queryModeller}
                      index={index}
                      operation={op}
                      query={query}
                      datasource={datasource}
                      onChange={onOperationChange}
                      onRemove={onRemove}
                      onRunQuery={onRunQuery}
                    />
                  ))}
                  {provided.placeholder}
                </div>
              )}
            </Droppable>
          </DragDropContext>
        )}
        <div className={styles.addButton}>
          {cascaderOpen ? (
            <Cascader
              options={addOptions}
              onSelect={onAddOperation}
              onBlur={onCascaderBlur}
              autoFocus={true}
              alwaysOpen={true}
              hideActiveLevelLabel={true}
              placeholder={'Search'}
            />
          ) : (
            <Button icon={'plus'} variant={'secondary'} onClick={() => setCascaderOpen(true)} title={'Add operation'}>
              Operations
            </Button>
          )}
        </div>
      </Stack>
    </Stack>
  );
}

const getStyles = (theme: GrafanaTheme2) => {
  return {
    heading: css({
      label: 'heading',
      fontSize: 12,
      fontWeight: theme.typography.fontWeightMedium,
      marginBottom: 0,
    }),
    operationList: css({
      label: 'operationList',
      display: 'flex',
      flexWrap: 'wrap',
      gap: theme.spacing(2),
    }),
    addButton: css({
      label: 'addButton',
      width: 126,
      paddingBottom: theme.spacing(1),
    }),
  };
};