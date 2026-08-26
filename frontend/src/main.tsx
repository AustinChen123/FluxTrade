import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./app/App";
import "./i18n";
import "./styles.css";
import { applyTheme, initialTheme } from "./theme";

applyTheme(initialTheme());
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
