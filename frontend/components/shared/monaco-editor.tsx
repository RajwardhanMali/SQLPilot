"use client";

import { useEffect, useRef, useState } from "react";
import * as monaco from "monaco-editor";

interface MonacoEditorProps {
  language: string;
  value: string;
  onChange?: (value: string | undefined) => void;
  readOnly?: boolean;
}

export default function MonacoEditor({
  language,
  value,
  onChange,
  readOnly = false,
}: MonacoEditorProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const [editor, setEditor] =
    useState<monaco.editor.IStandaloneCodeEditor | null>(null);

  useEffect(() => {
    if (editorRef.current) {
      // Define custom theme
      monaco.editor.defineTheme("sqlPilot", {
        base: "vs-dark",
        inherit: true,
        rules: [
          { token: "keyword", foreground: "7B61FF", fontStyle: "bold" },
          { token: "string", foreground: "FF71C0" },
          { token: "number", foreground: "2DD4D1" },
          { token: "comment", foreground: "6A7280", fontStyle: "italic" },
        ],
        colors: {
          "editor.background": "#1A1A2E",
          "editor.foreground": "#E0E0E0",
          "editorCursor.foreground": "#7B61FF",
          "editor.lineHighlightBackground": "#2A2A3E",
          "editorLineNumber.foreground": "#6A7280",
          "editor.selectionBackground": "#7B61FF40",
          "editor.inactiveSelectionBackground": "#7B61FF20",
        },
      });

      const newEditor = monaco.editor.create(editorRef.current, {
        value,
        language,
        theme: "sqlPilot",
        automaticLayout: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        readOnly,
        fontSize: 14,
        lineNumbers: "on",
        roundedSelection: true,
        scrollbar: {
          useShadows: false,
          verticalHasArrows: false,
          horizontalHasArrows: false,
          vertical: "visible",
          horizontal: "visible",
          verticalScrollbarSize: 10,
          horizontalScrollbarSize: 10,
        },
      });

      setEditor(newEditor);

      return () => {
        if (newEditor) {
          newEditor.dispose();
        }
      };
    }
  }, [editorRef, language, readOnly]);

  useEffect(() => {
    if (editor) {
      const model = editor.getModel();
      if (model && model.getValue() !== value) {
        editor.setValue(value);
      }

      const subscription = editor.onDidChangeModelContent(() => {
        if (onChange) {
          onChange(editor.getValue());
        }
      });

      return () => {
        subscription.dispose();
      };
    }
  }, [editor, value, onChange]);

  return <div ref={editorRef} className="h-full w-full" />;
}
