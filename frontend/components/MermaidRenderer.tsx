"use client";

import { useEffect, useRef } from "react";
import mermaid from "mermaid";

const MermaidRenderer = ({ chart }) => {
  const mermaidRef = useRef(null);

  useEffect(() => {
    mermaid.initialize({ startOnLoad: true });
    if (mermaidRef.current) {
      mermaid.contentLoaded();
    }
  }, [chart]);

  return <div ref={mermaidRef} className="mermaid">{chart}</div>;
};

export default MermaidRenderer;
