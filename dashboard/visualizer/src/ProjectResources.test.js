import { fireEvent, render, screen } from '@testing-library/react';
import ProjectResources, { resolveReport } from './ProjectResources';

test('a missing report leaves useful resources without a PDF placeholder or embedded media', () => {
  const { container } = render(<ProjectResources resources={{ report: null }} publicUrl="/HarbourSense" dataSource="replay" />);
  expect(screen.getByRole('region', { name: 'About this project' })).toBeInTheDocument();
  expect(screen.getByRole('navigation', { name: 'Project resources' })).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /Engineering case study/ })).toHaveAttribute('href', 'https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/CASE_STUDY.md');
  expect(screen.getByRole('link', { name: /Watch the walkthrough/ })).toHaveAttribute('href', '/HarbourSense/media/harboursense-walkthrough.webm');
  expect(screen.getByRole('link', { name: /Portfolio integration guide/ })).toHaveAttribute('href', 'https://github.com/Daiyan-Khan/HarbourSense/blob/main/docs/PORTFOLIO_INTEGRATION.md');
  expect(screen.queryByRole('button', { name: 'View report' })).not.toBeInTheDocument();
  expect(screen.queryByText('Original written report')).not.toBeInTheDocument();
  expect(container.querySelector('iframe, video, embed, object')).toBeNull();
});

test.each(['live', 'demo'])('ordinary %s builds link to the hosted walkthrough without embedding or fetching it', dataSource => {
  const { container } = render(<ProjectResources resources={{ report: null }} dataSource={dataSource} />);
  expect(screen.getByRole('link', { name: /Watch the walkthrough/ })).toHaveAttribute('href', 'https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm');
  expect(container.querySelector('iframe, video, embed, object')).toBeNull();
});

test('a corrected edition uses its supplied label and description instead of claiming an unchanged original', () => {
  const description = 'Adapted from the original academic report with corrected figures and reproducible evaluations.';
  const resources = { report: { title: 'HarbourSense project report', file: 'harboursense-report.pdf', pages: 32, edition: 'Corrected public edition', description } };
  render(<ProjectResources resources={resources} />);
  expect(screen.getByText('Corrected public edition')).toBeInTheDocument();
  expect(screen.getByText(description, { exact: false })).toBeInTheDocument();
  expect(screen.queryByText('Original written report')).not.toBeInTheDocument();
  expect(screen.queryByText(/describes the project at submission time/)).not.toBeInTheDocument();
  expect(screen.getByRole('heading', { name: 'HarbourSense project report' })).toBeInTheDocument();
});

test('the original report has distinct links and loads its viewer only after explicit activation', () => {
  const resources = { report: { title: 'Original project report', file: 'harboursense-report.pdf', pages: 32 } };
  const { container } = render(<ProjectResources resources={resources} publicUrl="/HarbourSense/" />);
  expect(screen.getByText('Original written report')).toBeInTheDocument();
  expect(screen.getByText('PDF · 32 pages')).toBeInTheDocument();
  expect(screen.getByRole('link', { name: /Open report/ })).toHaveAttribute('href', '/HarbourSense/reports/harboursense-report.pdf');
  expect(screen.getByRole('link', { name: 'Download PDF' })).toHaveAttribute('download');
  expect(container.querySelector('iframe')).toBeNull();
  fireEvent.click(screen.getByRole('button', { name: 'View report' }));
  expect(screen.getByTitle('Original project report PDF')).toHaveAttribute('src', '/HarbourSense/reports/harboursense-report.pdf');
  expect(screen.getByRole('button', { name: 'Close report viewer' })).toHaveAttribute('aria-expanded', 'true');
  fireEvent.click(screen.getByRole('button', { name: 'Close report viewer' }));
  expect(container.querySelector('iframe')).toBeNull();
});

test.each(['../private.pdf', '/other/report.pdf', 'https://external.test/report.pdf', 'report.pdf?token=value', 'report.html', '..pdf'])('rejects a report file outside the packaged PDF contract: %s', file => {
  expect(resolveReport({ file }, '/HarbourSense')).toBeNull();
});

test.each(['', '/'])('PDF links work when the app is hosted at the domain root: %s', publicUrl => {
  expect(resolveReport({ file: 'harboursense-report.pdf' }, publicUrl)?.url).toBe('/reports/harboursense-report.pdf');
});
