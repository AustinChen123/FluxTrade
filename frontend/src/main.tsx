import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./app/App";
import "./shared/i18n";
import { applyTheme, initialTheme } from "./shared/theme";
import "./styles/index.css";

applyTheme(initialTheme());
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
