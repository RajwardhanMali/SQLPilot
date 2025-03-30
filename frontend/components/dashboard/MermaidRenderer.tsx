"use client";

import { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";
import { RefreshCcwDot } from "lucide-react";

const MermaidRenderer = ({ chart }: { chart: string }) => {
  const mermaidRef = useRef<HTMLDivElement>(null);
  const [renderTrigger, setRenderTrigger] = useState(0); // State to trigger re-render

  const handleRender = () => {
    setRenderTrigger((prev) => prev + 1); // Increment to trigger re-render
  };

  useEffect(() => {
    if (mermaidRef.current) {
      mermaid.initialize({
        startOnLoad: false,
        theme: "dark", // Can also use 'dark', 'forest', etc.
        themeVariables: {
          primaryColor: "#1f77b4", // Node background color
          edgeStroke: "#000", // Relationship line color
        },
      });

      mermaid
        .render("mermaid-chart", chart)
        .then(({ svg }) => {
          if (mermaidRef.current) {
            mermaidRef.current.innerHTML = svg; // Render the chart
          }
        })
        .catch((error) => {
          console.error("Error rendering Mermaid chart:", error);
        });
    }
  }, [chart, renderTrigger]); // Re-render when chart or renderTrigger changes

  return (
    <div className="relative w-full h-full ">
      <div ref={mermaidRef} className="mermaid" />
      <button
        onClick={handleRender}
        className="mt-2 p-4 bg-gradient-to-r from-purple-600 to-pink-600 text-white absolute top-5 right-5 rounded-full"
      >
        <RefreshCcwDot className="hover:animate-spin transition" />
      </button>
    </div>
  );
};

export default MermaidRenderer;
