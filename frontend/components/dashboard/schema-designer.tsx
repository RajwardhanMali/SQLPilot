"use client";

import { useState, useRef, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Sparkles, Lightbulb, X, Loader2, Loader } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card } from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import MonacoEditor from "@/components/shared/monaco-editor";
import { saveSchema } from "@/actions/schemas";
import useDashboardStore from "@/store/dashboard";
import { useParams } from "next/navigation";
import MermaidRenderer from "@/components/dashboard/MermaidRenderer";
import { Input } from "../ui/input";

export default function SchemaDesigner() {
  const [isGenerating, setIsGenerating] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);
  const [copied, setCopied] = useState(false);
  const [naturalLanguageInput, setNaturalLanguageInput] = useState("");
  const [generatedSchema, setGeneratedSchema] = useState("");
  const [schemaVisualization, setSchemaVisualization] = useState("");
  const [showTips, setShowTips] = useState(true);
  const [showExamples, setShowExamples] = useState(false);
  const [isTyping, setIsTyping] = useState(false);
  const typingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const [name, setName] = useState("");
  const { id } = useParams();
  const schema = useDashboardStore((state) => state.schema);
  useEffect(() => {
    if (schema && typeof schema.ddl === "string") {
      setHasGenerated(true);
      setGeneratedSchema(schema.ddl);
      setSchemaVisualization(schema.mermaid);
      setName(schema.name);
    } else {
      setGeneratedSchema("");
      setHasGenerated(false);
    }
  }, [schema]);

  const examples = [
    "Create a schema for an e-commerce platform with products, categories, customers, and orders",
    "Design a database for a blog with users, posts, comments, and tags",
    "Build a schema for a hospital management system with patients, doctors, appointments, and medical records",
    "Create an analytics database optimized for OLAP queries on customer behavior",
  ];

  const tips = [
    "Be specific about relationships between entities",
    "Mention performance requirements (OLTP vs OLAP)",
    "Specify data types and constraints when needed",
    "Include partitioning strategies for large tables",
  ];

  useEffect(() => {
    if (typingTimeoutRef.current) {
      clearTimeout(typingTimeoutRef.current);
    }

    if (naturalLanguageInput) {
      setIsTyping(true);
      typingTimeoutRef.current = setTimeout(() => {
        setIsTyping(false);
      }, 1000);
    } else {
      setIsTyping(false);
    }

    return () => {
      if (typingTimeoutRef.current) {
        clearTimeout(typingTimeoutRef.current);
      }
    };
  }, [naturalLanguageInput]);

  const handleGenerate = async () => {
    if (!naturalLanguageInput.trim()) return;

    setIsGenerating(true);

    try {
      const response = await fetch("http://10.1.50.35:5000/generate_schema", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          business_description: naturalLanguageInput,
          dialect: "trino",
        }),
      });
      const data = await response.json();
      // const data: any = schemaResponse;
      setGeneratedSchema(data.ddl_statements.join("\n\n"));
      setSchemaVisualization(data.schema.mermaid_code);
      setHasGenerated(true);
    } catch (error) {
      console.error("Error generating schema:", error);
    } finally {
      setIsGenerating(false);
    }
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(generatedSchema);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleExampleClick = (example: string) => {
    setNaturalLanguageInput(example);
    setShowExamples(false);
  };

  const handleSave = async () => {
    if (!generatedSchema.trim()) return;
    setIsSaving(true);
    try {
      const schema = {
        name,
        ddl: generatedSchema,
        project: id as string,
        mermaid: schemaVisualization,
      };
      await saveSchema(schema);
      const statements = generatedSchema.split("\n\n");
      await Promise.all(
        statements.map(async (ddl) => {
          const res = await fetch(`http://10.1.50.35:5000/execute_query`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              sql_query: ddl,
              schema: schema.name.toLowerCase(),
              dialect: "trino",
            }),
          });
          console.log(res);
        })
      );
    } catch (error) {
      console.log(error);
    } finally {
      setIsSaving(false);
    }
  };

  const renderSchemaVisualization = () => {
    return (
      <div className="min-h-[500px] flex items-center justify-center border-2 border-dashed border-white/10 rounded-md">
        {hasGenerated ? (
          <MermaidRenderer
            chart={schemaVisualization || (schema?.mermaid as string)}
          />
        ) : (
          <p className="text-muted-foreground text-sm">
            No schema generated yet. Start by describing your requirements.
          </p>
        )}
      </div>
    );
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
          <div className="p-6 border-b border-white/10">
            <div className="flex justify-between items-center">
              <h3 className="text-xl font-display font-bold neon-gradient-text">
                Schema Requirements
              </h3>
              <Input
                type="text"
                placeholder="Enter name for schema"
                value={name}
                className="w-[180px]"
                onChange={(e) => setName(e.target.value)}
              />
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      variant="ghost"
                      size="icon"
                      className="text-muted-foreground hover:text-white"
                      onClick={() => setShowTips(!showTips)}
                    >
                      <Lightbulb className="h-4 w-4" />
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Toggle writing tips</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </div>
            <p className="text-muted-foreground text-sm mt-1">
              Describe your database needs in natural language
            </p>
          </div>

          <div className="p-6">
            <div className="relative">
              <Textarea
                placeholder="Example: Create a schema for an e-commerce platform with products, categories, customers, and orders. Include tables for inventory tracking and optimize for OLAP queries on sales data."
                className="min-h-[200px] bg-dark-200/50 border-white/10 focus:border-neon-purple transition-all resize-none"
                value={naturalLanguageInput}
                onChange={(e) => setNaturalLanguageInput(e.target.value)}
              />

              {isTyping && (
                <div className="absolute bottom-3 right-3 text-xs text-muted-foreground">
                  Typing...
                </div>
              )}

              <AnimatePresence>
                {showExamples && (
                  <motion.div
                    className="absolute inset-0 bg-dark-100/95 backdrop-blur-md rounded-md border border-white/10 p-4 overflow-auto custom-scrollbar"
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: 10 }}
                    transition={{ duration: 0.2 }}
                  >
                    <div className="flex justify-between items-center mb-3">
                      <h4 className="font-medium">Example Prompts</h4>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6"
                        onClick={() => setShowExamples(false)}
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                    <div className="space-y-2">
                      {examples.map((example, index) => (
                        <motion.button
                          key={index}
                          className="w-full text-left p-3 rounded-md bg-dark-200/50 hover:bg-dark-200 border border-white/5 hover:border-white/10 transition-colors"
                          onClick={() => handleExampleClick(example)}
                          whileHover={{ x: 5 }}
                        >
                          {example}
                        </motion.button>
                      ))}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            <AnimatePresence>
              {showTips && (
                <motion.div
                  className="mt-4 bg-dark-200/50 rounded-md p-4 border border-white/10"
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  exit={{ opacity: 0, height: 0 }}
                  transition={{ duration: 0.3 }}
                >
                  <div className="flex items-center gap-2 mb-2">
                    <Lightbulb className="h-4 w-4 text-neon-yellow" />
                    <h4 className="font-medium text-base">Writing Tips</h4>
                  </div>
                  <ul className="space-y-1 text-sm text-muted-foreground">
                    {tips.map((tip, index) => (
                      <motion.li
                        key={index}
                        className="flex items-start gap-2"
                        initial={{ opacity: 0, x: -10 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: index * 0.1 }}
                      >
                        <span className="text-neon-teal">•</span>
                        <span>{tip}</span>
                      </motion.li>
                    ))}
                  </ul>
                </motion.div>
              )}
            </AnimatePresence>
          </div>

          <div className="px-6 py-4 border-t border-white/10 flex justify-between">
            <Button
              variant="outline"
              className="neon-border bg-dark-200/50"
              onClick={() => setShowExamples(true)}
            >
              Load Example
            </Button>
            <Button
              onClick={handleGenerate}
              disabled={isGenerating || !naturalLanguageInput.trim()}
              className="neon-gradient-bg hover:opacity-90 transition-opacity"
            >
              {isGenerating ? (
                <div className="flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Generating...
                </div>
              ) : (
                <div className="flex items-center gap-2">
                  <Sparkles className="h-4 w-4" />
                  Generate Schema
                </div>
              )}
            </Button>
          </div>
        </Card>

        <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
          <div className="p-6 border-b border-white/10 flex justify-between">
            <h3 className="text-xl font-display font-bold neon-gradient-text">
              Schema Editor
            </h3>
            <div className="flex gap-2">
              <Button
                onClick={handleSave}
                className="neon-gradient-bg hover:opacity-90 transition-opacity"
              >
                {isSaving ? (
                  <Loader className="animate-spin transition size-4" />
                ) : (
                  "Execute And Save"
                )}
              </Button>
            </div>
          </div>

          <div className="p-6">
            <div className="relative h-[calc(100vh-350px)] border rounded-md overflow-hidden">
              <MonacoEditor
                language="sql"
                value={generatedSchema}
                onChange={(value) => setGeneratedSchema(value || "")}
              />
            </div>
          </div>

          {hasGenerated && (
            <div className="px-6 py-4 border-t border-white/10 flex justify-end">
              <Button
                variant="outline"
                className="neon-border bg-dark-200/50"
                onClick={handleCopy}
              >
                {copied ? "Copied!" : "Copy Schema"}
              </Button>
            </div>
          )}
        </Card>
      </div>

      <div className="grid grid-cols-1 gap-6">
        <Card className="bg-dark-100/50 backdrop-blur-md border border-white/10 overflow-hidden">
          <div className="p-6 border-b border-white/10">
            <h3 className="text-xl font-display font-bold neon-gradient-text">
              Schema Visualization
            </h3>
          </div>

          <div className="p-6">{renderSchemaVisualization()}</div>
        </Card>
      </div>
    </div>
  );
}
