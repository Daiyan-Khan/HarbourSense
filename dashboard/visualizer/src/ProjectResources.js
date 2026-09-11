import React, { useState } from 'react';
import projectResources from './projectResources.json';

const REPOSITORY = 'https://github.com/Daiyan-Khan/HarbourSense';
const HOSTED_WALKTHROUGH = 'https://daiyan-khan.github.io/HarbourSense/media/harboursense-walkthrough.webm';

function assetUrl(publicUrl, file) {
  return `${(publicUrl || '').replace(/\/+$/, '')}/${file}`;
}

export function resolveReport(report, publicUrl) {
  if (!report || typeof report.file !== 'string' || !/^[a-z0-9][a-z0-9_-]*\.pdf$/i.test(report.file)) return null;
  const edition = typeof report.edition === 'string' ? report.edition.trim() : '';
  return {
    title: typeof report.title === 'string' && report.title.trim() ? report.title.trim() : (edition ? 'Project report' : 'Original project report'),
    edition: edition || 'Original written report',
    description: typeof report.description === 'string' && report.description.trim() ? report.description.trim() : (edition
      ? 'A public edition adapted from the academic report. See the engineering case study for the current implementation and results.'
      : 'Original academic report; describes the project at submission time. Current results are in the engineering case study.'),
    url: assetUrl(publicUrl, `reports/${report.file}`),
    pages: Number.isInteger(report.pages) && report.pages > 0 ? report.pages : null,
  };
}

export default function ProjectResources({ resources = projectResources, publicUrl = process.env.PUBLIC_URL || '', dataSource = process.env.REACT_APP_DATA_SOURCE || 'live' }) {
  const [showReport, setShowReport] = useState(false);
  const report = resolveReport(resources?.report, publicUrl);
  const links = [
    { title: 'Engineering case study', description: 'Architecture, design decisions, evaluation and limitations.', url: `${REPOSITORY}/blob/main/docs/CASE_STUDY.md` },
    { title: 'Source code', description: 'Explore the services, dashboard and reproducible demo.', url: REPOSITORY },
    { title: 'Watch the walkthrough', description: 'Follow a recorded shipment from arrival to delivery.', url: dataSource === 'replay' ? assetUrl(publicUrl, 'media/harboursense-walkthrough.webm') : HOSTED_WALKTHROUGH },
    { title: 'Portfolio integration guide', description: 'Link this project from a personal site or portfolio.', url: `${REPOSITORY}/blob/main/docs/PORTFOLIO_INTEGRATION.md` },
  ];

  return <section id="project-resources" className="project-resources" aria-labelledby="project-resources-title">
    <div className="project-resources-heading"><span className="eyebrow">Explore the project</span><h2 id="project-resources-title">About this project</h2><p>Go beyond the port view: read the engineering story, explore the code, or watch the demo.</p></div>
    {report && <div className="project-report">
      <div className="project-report-intro"><span className="eyebrow">{report.edition}</span><h3>{report.title}</h3><p>{report.description}{report.pages && <span className="report-page-count"> PDF · {report.pages} pages</span>}</p></div>
      <div className="project-report-actions">
        <button type="button" className="primary-button" aria-expanded={showReport} aria-controls="project-report-viewer" onClick={() => setShowReport(!showReport)}>{showReport ? 'Close report viewer' : 'View report'}</button>
        <a href={report.url} target="_blank" rel="noopener noreferrer">Open report<span className="sr-only"> in a new tab</span> <span aria-hidden="true">↗</span></a>
        <a href={report.url} download>Download PDF</a>
      </div>
      <div id="project-report-viewer" className="project-report-viewer" hidden={!showReport}>
        {showReport && <iframe src={report.url} title={`${report.title} PDF`} />}
      </div>
    </div>}
    <nav className="project-resource-links" aria-label="Project resources">
      {links.map(link => <a className="project-resource-link" key={link.title} href={link.url} target="_blank" rel="noopener noreferrer"><span className="project-resource-link-title">{link.title}<span className="sr-only"> in a new tab</span><span aria-hidden="true">↗</span></span><span>{link.description}</span></a>)}
    </nav>
  </section>;
}
