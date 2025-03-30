"use client";

import { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";

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
        className="mt-2 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 absolute top-5 right-5"
      >
        Re-render Chart
      </button>
    </div>
  );
};

export default MermaidRenderer;
