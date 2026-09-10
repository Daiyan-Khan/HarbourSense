import React, { createContext, useCallback, useContext, useEffect, useMemo, useReducer, useRef } from 'react';
import { createLiveSource } from '../data/live';
import { createReplaySource } from '../data/replay';
import {
  initialState, portDashboardReducer, selectFlowEdges, selectFlowNodes,
  selectHasAnimatingDevices, selectNodeSensors, selectPortStateView, selectSelectedDevice,
} from './portDashboardStore';

const PortDashboardContext = createContext(null);

export function PortDashboardProvider({ children, dataSource = process.env.REACT_APP_DATA_SOURCE || 'live', sourceFactory }) {
  const [state, dispatch] = useReducer(portDashboardReducer, initialState);
  const adapterRef = useRef(null);

  useEffect(() => {
    const factory = sourceFactory || (dataSource === 'replay' ? createReplaySource : createLiveSource);
    const source = factory({ emit: dispatch });
    adapterRef.current = source;
    source.start();
    return () => { source.dispose(); adapterRef.current = null; };
  }, [dataSource, sourceFactory]);

  const animating = selectHasAnimatingDevices(state);
  const frozen = state.source.kind === 'replay' || ['paused', 'complete'].includes(state.source.status);
  useEffect(() => {
    if (frozen) return undefined;
    const clock = window.setInterval(() => dispatch({ type: 'TICK_FRAME', payload: Date.now() }), 1000);
    return () => window.clearInterval(clock);
  }, [frozen]);

  useEffect(() => {
    if (frozen || !animating || window.matchMedia?.('(prefers-reduced-motion: reduce)').matches) return undefined;
    let raf;
    const tick = () => {
      dispatch({ type: 'TICK_FRAME', payload: Date.now() });
      raf = window.requestAnimationFrame(tick);
    };
    raf = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(raf);
  }, [animating, frozen]);

  const selectDevice = useCallback((deviceOrId) => {
    const id = typeof deviceOrId === 'string' ? deviceOrId : deviceOrId?.id ?? null;
    dispatch({ type: 'SELECT_DEVICE', payload: id });
  }, []);
  const selectSensorNode = useCallback((id) => dispatch({ type: 'SELECT_SENSOR_NODE', payload: id }), []);
  const selectShipment = useCallback((id) => dispatch({ type: 'SELECT_SHIPMENT', payload: id }), []);
  const command = useCallback((action, value) => adapterRef.current?.command(action, value), []);
  const chooseScenario = useCallback((id) => adapterRef.current?.choose(id), []);
  const retry = useCallback(() => adapterRef.current?.retry(), []);
  const value = useMemo(() => ({
    state, dispatch, selectDevice, selectSensorNode, selectShipment, command, chooseScenario, retry,
    flowNodes: selectFlowNodes(state), flowEdges: selectFlowEdges(state),
    portStateView: selectPortStateView(state), selectedDevice: selectSelectedDevice(state),
    nodeSensors: selectNodeSensors(state), frameNow: state.display.frameNow,
  }), [state, selectDevice, selectSensorNode, selectShipment, command, chooseScenario, retry]);

  return <PortDashboardContext.Provider value={value}>{children}</PortDashboardContext.Provider>;
}

export function usePortDashboard() {
  const context = useContext(PortDashboardContext);
  if (!context) throw new Error('usePortDashboard must be used within PortDashboardProvider');
  return context;
}
