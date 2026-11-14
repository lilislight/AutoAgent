import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@xyflow/react/dist/style.css";
import "./styles.css";
import App from "./App";
import { TooltipProvider } from './components/Tooltip';
import { installEnglishValidation } from './englishValidation';

const removeEnglishValidation = installEnglishValidation(document);
if (import.meta.hot) import.meta.hot.dispose(removeEnglishValidation);
const client = new QueryClient({ defaultOptions: { queries: { retry: 1, gcTime: 60_000 } } });
createRoot(document.getElementById("root")!).render(<StrictMode><QueryClientProvider client={client}><TooltipProvider><App /></TooltipProvider></QueryClientProvider></StrictMode>);
