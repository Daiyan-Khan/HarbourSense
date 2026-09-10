import { fireEvent, render, screen, within } from '@testing-library/react';
import { DetailDrawer, SensorDetailModal } from './DeviceDetailModal';

test('drawer receives focus, traps Tab, closes with Escape and restores its trigger', () => {
  const trigger = document.createElement('button');
  trigger.textContent = 'Inspect'; document.body.appendChild(trigger); trigger.focus();
  const onClose = jest.fn();
  const { unmount } = render(<DetailDrawer label="Test details" title="Truck" onClose={onClose}><button>Inner action</button></DetailDrawer>);
  const dialog = screen.getByRole('dialog', { name: 'Test details' });
  const buttons = within(dialog).getAllByRole('button');
  expect(buttons[0]).toHaveFocus();
  fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
  expect(buttons[buttons.length - 1]).toHaveFocus();
  fireEvent.keyDown(document, { key: 'Tab' });
  expect(buttons[0]).toHaveFocus();
  fireEvent.keyDown(document, { key: 'Escape' });
  expect(onClose).toHaveBeenCalledTimes(1);
  unmount();
  expect(trigger).toHaveFocus();
  trigger.remove();
});

test('locations without sensors still open an informative, closable drawer', () => {
  render(<SensorDetailModal sensors={[]} nodeId="A1" onClose={() => {}} />);
  expect(screen.getByRole('dialog', { name: 'Sensors at node' })).toHaveTextContent('No sensor readings are available');
  expect(screen.getByRole('button', { name: 'Close details' })).toHaveFocus();
});
