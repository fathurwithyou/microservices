import { NavLink, Route, Routes } from "react-router-dom";
import LiveHazards from "./pages/LiveHazards";
import Overview from "./pages/Overview";
import ScenarioLab from "./pages/ScenarioLab";
import TestEvidence from "./pages/TestEvidence";
import SessionBar from "./components/SessionBar";
import EvidenceLog from "./components/EvidenceLog";

export default function App() {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-mark" title="BNPB M1">G</div>
        <div className="brand">IF4031 · M1</div>
        <h1 className="brand-title">BNPB Demo Console</h1>
        <nav className="nav">
          <NavLink to="/" end title="Overview">OV</NavLink>
          <NavLink to="/hazards" title="Live Hazards">HZ</NavLink>
          <NavLink to="/lab" title="Scenario Lab P1–P5">LAB</NavLink>
          <NavLink to="/evidence" title="Test Evidence">EV</NavLink>
        </nav>
        <p>Tiga identitas client terpisah. Setiap klaim diikat ke HTTP status, correlation ID, dan assertion otomatis — bukan tombol lulus manual.</p>
      </aside>
      <header className="topbar">
        <div className="topbar-title">
          <h1>BNPB Demo Console</h1>
          <span>IF4031 · disaster coordination dashboard</span>
        </div>
        <span className="live-dot" title="Live" />
      </header>
      <main className="main">
        <SessionBar />
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/hazards" element={<LiveHazards />} />
          <Route path="/lab" element={<ScenarioLab />} />
          <Route path="/evidence" element={<TestEvidence />} />
        </Routes>
        <EvidenceLog />
      </main>
    </div>
  );
}
