import { useEffect, useState } from "react";
import { CLIENTS, canRequestRaw, formatCountdown } from "../lib/clients";
import { useSessions } from "../state/sessions";
import type { ClientId } from "../types";

export default function SessionBar() {
  const { sessions, activeClientId, setActiveClientId, login, refresh, logout } = useSessions();
  const [, setTick] = useState(0);

  useEffect(() => {
    const timer = window.setInterval(() => setTick((value) => value + 1), 1000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <section className="grid cols-3" style={{ marginBottom: "1rem" }}>
      {CLIENTS.map((client) => {
        const session = sessions[client.id];
        return (
          <article
            key={client.id}
            className={`panel session-card ${activeClientId === client.id ? "selected" : ""}`}
            onClick={() => setActiveClientId(client.id)}
          >
            <h3>{client.label}</h3>
            <p className="kicker">{client.id}</p>
            <p>{client.description}</p>
            {session ? (
              <>
                <p className={`stat ${formatCountdown(session.expiresAt) === "expired" ? "expired" : ""}`}>
                  {formatCountdown(session.expiresAt)}
                </p>
                <p>
                  <span className="pill">{canRequestRaw(session.scopes) ? "raw scope" : "summary only"}</span>
                </p>
                <p className="mono">{session.scopes.join(", ")}</p>
                <div className="row">
                  <button type="button" onClick={() => refresh(client.id)}>Refresh</button>
                  <button type="button" onClick={() => logout(client.id)}>Logout</button>
                </div>
              </>
            ) : (
              <LoginForm clientId={client.id} onLogin={login} />
            )}
          </article>
        );
      })}
    </section>
  );
}

function LoginForm({
  clientId,
  onLogin,
}: {
  clientId: ClientId;
  onLogin: (clientId: ClientId, secret: string) => Promise<{ ok: boolean; message: string }>;
}) {
  const [secret, setSecret] = useState("");
  const [message, setMessage] = useState("");
  return (
    <form
      onSubmit={async (event) => {
        event.preventDefault();
        const result = await onLogin(clientId, secret);
        setMessage(result.message);
        setSecret("");
      }}
    >
      <input
        type="password"
        autoComplete="off"
        placeholder="client_secret dari .env"
        value={secret}
        onChange={(event) => setSecret(event.target.value)}
      />
      <div className="row" style={{ marginTop: "0.5rem" }}>
        <button className="primary" type="submit">Issue token</button>
      </div>
      {message ? <p className="kicker">{message}</p> : null}
    </form>
  );
}
