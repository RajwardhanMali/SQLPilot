import MermaidRenderer from "@/components/MermaidRenderer";

export default function MermaidPage() {
  const mermaidDiagram = `
  erDiagram
    fact_customer_behavior {
      INT customer_id
      INT product_id
      INT time_id
      INT channel_id
      INT quantity
      DECIMAL revenue
    }
    dim_customers {
      INT customer_id PK
      VARCHAR customer_name
      VARCHAR city
      VARCHAR state
      VARCHAR country
    }
    dim_products {
      INT product_id PK
      VARCHAR product_name
      VARCHAR category
      DECIMAL price
    }
    dim_time {
      INT time_id PK
      DATE date
      VARCHAR day_of_week
      VARCHAR month
      INT year
    }
    dim_channels {
      INT channel_id PK
      VARCHAR channel_name
    }

    fact_customer_behavior ||--|{ dim_customers : "references"
    fact_customer_behavior ||--|{ dim_products : "references"
    fact_customer_behavior ||--|{ dim_time : "references"
    fact_customer_behavior ||--|{ dim_channels : "references"
  `;

  return (
    <div>
      <h1>Mermaid ER Diagram</h1>
      <MermaidRenderer chart={mermaidDiagram} />
    </div>
  );
}
