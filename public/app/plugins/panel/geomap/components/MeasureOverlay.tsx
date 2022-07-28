import { css } from '@emotion/css';
import Map from 'ol/Map';
import { Geometry, LineString, Point } from 'ol/geom';
import { Draw, Modify } from 'ol/interaction';
import { Vector as VectorLayer } from 'ol/layer';
import { Vector as VectorSource } from 'ol/source';
import { getArea, getLength } from 'ol/sphere';
import React, { useState } from 'react';
import tinycolor from 'tinycolor2';

import { GrafanaTheme } from '@grafana/data';
import { IconButton, RadioButtonGroup, stylesFactory } from '@grafana/ui';
import { config } from 'app/core/config';

import { style, labelStyle, tipStyle, modifyStyle, segmentStyle } from '../globalStyles';

type Props = {
  map: Map;
  menuActiveState: (value: boolean) => void;
};

export const MeasureOverlay = ({ map, menuActiveState }: Props) => {
  const measureStyle = getStyles(config.theme);

  // Menu State Management
  const [firstLoad, setFirstLoad] = useState<boolean>(true);
  const [menuActive, setMenuActive] = useState<boolean>(false);

  // Options State
  const [typeSelect, setTypeSelect] = useState<string>('LineString');
  const clearPrevious = true;
  const showSegments = false;

  function toggleMenu() {
    setMenuActive(!menuActive);
    // Lift menu state
    // TODO: consolidate into one state
    menuActiveState(!menuActive);
    if (menuActive) {
      map.removeInteraction(draw);
      vector.set('visible', false);
    } else {
      if (firstLoad) {
        // Initialize on first load
        setFirstLoad(false);
        map.addLayer(vector);
        map.addInteraction(modify);
      }
      vector.set('visible', true);
      map.removeInteraction(draw); // Remove last interaction
      addInteraction(map, typeSelect, showSegments, clearPrevious);
    }
  }

  return (
    <div className={`${measureStyle.infoWrap} ol-unselectable ol-control`} style={{ backgroundColor: '#22252b' }}>
      <IconButton
        name="ruler-combined"
        style={{ backgroundColor: 'rgba(204, 204, 220, 0.16)', display: 'inline-block', marginRight: '2px' }}
        tooltip={`${menuActive ? 'hide' : 'show'} measure tools`}
        tooltipPlacement="top"
        onClick={() => {
          toggleMenu();
        }}
      />
      {menuActive ? (
        <>
          <IconButton
            name="angle-right"
            style={{ float: 'left' }}
            tooltip="hide measure tools"
            tooltipPlacement="left"
            onClick={() => {
              toggleMenu();
            }}
          />
          <RadioButtonGroup
            value={typeSelect}
            options={[
              { label: 'area', value: 'Polygon' },
              { label: 'length', value: 'LineString' },
            ]}
            size="sm"
            onChange={(e) => {
              map.removeInteraction(draw);
              setTypeSelect(e);
              addInteraction(map, e, showSegments, clearPrevious);
            }}
          />
        </>
      ) : null}
    </div>
  );
};

const segmentStyles = [segmentStyle];
const source = new VectorSource();
const vector = new VectorLayer({
  source: source,
  style: function (feature) {
    return styleFunction(feature, false);
  },
  visible: true,
});

const modify = new Modify({ source: source, style: modifyStyle });
let tipPoint: Geometry;
let draw: Draw; // global so we can remove it later

const formatLength = function (line: Geometry) {
  const length = getLength(line);
  let output;
  if (length > 100) {
    output = Math.round((length / 1000) * 100) / 100 + ' km';
  } else {
    output = Math.round(length * 100) / 100 + ' m';
  }
  return output;
};

const formatArea = function (polygon: Geometry) {
  const area = getArea(polygon);
  let output;
  if (area > 10000) {
    output = Math.round((area / 1000000) * 100) / 100 + ' km\xB2';
  } else {
    output = Math.round(area * 100) / 100 + ' m\xB2';
  }
  return output;
};

// TODO: reconcile Feature type in open layers
// eslint-disable-next-line
function styleFunction(feature: any, segments: boolean, drawType?: string, tip?: string) {
  const styles = [style];
  const geometry = feature.getGeometry();
  if (geometry) {
    const type = geometry.getType();
    let point, label, line;
    if (!drawType || drawType === type) {
      if (type === 'Polygon') {
        point = geometry.getInteriorPoint();
        label = formatArea(geometry);
        line = new LineString(geometry.getCoordinates()[0]);
      } else if (type === 'LineString') {
        point = new Point(geometry.getLastCoordinate());
        label = formatLength(geometry);
        line = geometry;
      }
    }
    if (segments && line) {
      let count = 0;
      line.forEachSegment(function (a: number, b: number) {
        const segment = new LineString([a, b]);
        const label = formatLength(segment);
        if (segmentStyles.length - 1 < count) {
          segmentStyles.push(segmentStyle.clone());
        }
        const segmentPoint = new Point(segment.getCoordinateAt(0.5));
        segmentStyles[count].setGeometry(segmentPoint);
        segmentStyles[count].getText().setText(label);
        styles.push(segmentStyles[count]);
        count++;
      });
    }
    if (label) {
      labelStyle.setGeometry(point);
      labelStyle.getText().setText(label);
      styles.push(labelStyle);
    }
    if (tip && type === 'Point' && !modify.getOverlay().getSource().getFeatures().length) {
      tipPoint = geometry;
      tipStyle.getText().setText(tip);
      styles.push(tipStyle);
    }
  }

  return styles;
}

function addInteraction(map: Map, typeSelect: string, showSegments: boolean, clearPrevious: boolean) {
  const drawType = typeSelect;
  const activeTip =
    ' Click to continue ' + (drawType === 'Polygon' ? 'polygon' : 'line') + ' \n (double-click to end) ';
  const idleTip = ' Click to start ';
  let tip = idleTip;
  draw = new Draw({
    source: source,
    type: drawType,
    style: function (feature) {
      return styleFunction(feature, showSegments, drawType, tip);
    },
  });
  draw.on('drawstart', function () {
    if (clearPrevious) {
      source.clear();
    }
    modify.setActive(false);
    tip = activeTip;
  });
  draw.on('drawend', function () {
    modifyStyle.setGeometry(tipPoint);
    modify.setActive(true);
    map.once('pointermove', function () {
      modifyStyle.setGeometry('');
    });
    tip = idleTip;
  });
  modify.setActive(true);
  map.addInteraction(draw);
}

const getStyles = stylesFactory((theme: GrafanaTheme) => ({
  infoWrap: css`
    color: ${theme.colors.text};
    background: ${tinycolor(theme.colors.panelBg).setAlpha(0.7).toString()};
    border-radius: 2px;
    padding: 8px;
  `,
}));
