import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import { EvidenceProvider } from "./state/evidence";
import { SessionsProvider } from "./state/sessions";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <SessionsProvider>
        <EvidenceProvider>
          <App />
        </EvidenceProvider>
      </SessionsProvider>
    </BrowserRouter>
  </React.StrictMode>,
);
