import { SchemaType } from "@/actions/schemas";
import { create } from "zustand";

interface Connection {
  dialect: "Trino" | "Spark";
  host: string;
  port: number;
}

interface Project {
  _id: string;
  title: string;
}

interface DashboardState {
  connection: Connection;
  schema: SchemaType | null;
  project: Project | null;
  catalog: String | null;
  setConnection: (connection: Connection) => void;
  setSchema: (schema: SchemaType) => void;
  setProject: (project: Project) => void;
  setCatalog: (catalog: String) => void;
}

const useDashboardStore = create<DashboardState>((set) => ({
  connection: {
    dialect: "Trino",
    host: "localhost",
    port: 4032,
  },
  schema: null,
  project: null,
  catalog: null,
  setConnection: (connection) => set(() => ({ connection })),
  setSchema: (schema) => set(() => ({ schema })),
  setProject: (project) => set(() => ({ project })),
  setCatalog: (catalog) => set(() => ({ catalog })),
}));

export default useDashboardStore;
