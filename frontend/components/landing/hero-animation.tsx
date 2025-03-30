"use client";

import { useEffect, useRef, useState } from "react";
import { motion, useAnimation, useInView } from "framer-motion";

export default function HeroAnimation() {
  const ref = useRef(null);
  const isInView = useInView(ref, { once: true });
  const controls = useAnimation();
  const [isTyping, setIsTyping] = useState(false);
  const [currentStep, setCurrentStep] = useState(0);

  useEffect(() => {
    if (isInView) {
      controls.start("visible");

      // Start the typing animation sequence
      const typingSequence = async () => {
        setIsTyping(true);
        await new Promise((resolve) => setTimeout(resolve, 1000));
        setCurrentStep(1); // Show natural language input
        await new Promise((resolve) => setTimeout(resolve, 3000));
        setCurrentStep(2); // Show schema generation
        await new Promise((resolve) => setTimeout(resolve, 2000));
        setCurrentStep(3); // Show query input
        await new Promise((resolve) => setTimeout(resolve, 3000));
        setCurrentStep(4); // Show SQL generation
        setIsTyping(false);
      };

      typingSequence();
    }
  }, [isInView, controls]);

  const container = {
    hidden: { opacity: 0 },
    visible: {
      opacity: 1,
      transition: {
        delayChildren: 0.3,
        staggerChildren: 0.2,
      },
    },
  };

  const item = {
    hidden: { y: 20, opacity: 0 },
    visible: {
      y: 0,
      opacity: 1,
    },
  };

  const typingCursor = {
    hidden: { opacity: 0 },
    visible: {
      opacity: [0, 1, 0],
      transition: {
        duration: 0.8,
        repeat: Number.POSITIVE_INFINITY,
        repeatType: "loop",
      },
    },
  };

  const naturalLanguageInput =
    "Create a schema for an e-commerce platform with products, categories, customers, and orders. Include tables for inventory tracking and optimize for OLAP queries on sales data.";

  const generatedSchema = `CREATE TABLE categories (
  category_id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  category_id INT REFERENCES categories(category_id),
  name VARCHAR(200) NOT NULL,
  description TEXT,
  price DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITIONED BY (category_id);`;

  const naturalLanguageQuery =
    "Show me the top 5 product categories by sales revenue in the last quarter";

  const generatedSQL = `SELECT 
  c.name AS category_name,
  SUM(oi.quantity * oi.price) AS total_revenue
FROM 
  orders o
JOIN 
  order_items oi ON o.order_id = oi.order_id
JOIN 
  products p ON oi.product_id = p.product_id
JOIN 
  categories c ON p.category_id = c.category_id
WHERE 
  o.order_date >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3' MONTH
  AND o.order_date < DATE_TRUNC('quarter', CURRENT_DATE)
GROUP BY 
  c.name
ORDER BY 
  total_revenue DESC
LIMIT 5;`;

  return (
    <div
      ref={ref}
      className="relative w-full h-[600px] rounded-xl overflow-hidden bg-dark-100/50 backdrop-blur-md border border-white/10 shadow-2xl"
    >
      <div className="absolute inset-0 bg-gradient-to-r from-neon-purple/5 to-neon-blue/5 z-0"></div>
      <div className="absolute inset-0 grid-pattern opacity-20"></div>

      <motion.div
        className="absolute inset-0 flex flex-col p-8"
        variants={container}
        initial="hidden"
        animate={controls}
      >
        {/* Terminal Header */}
        <div className="bg-dark-200 rounded-t-lg p-3 flex items-center gap-2 border-b border-white/10">
          <div className="flex gap-1.5">
            <div className="h-3 w-3 rounded-full bg-red-500"></div>
            <div className="h-3 w-3 rounded-full bg-yellow-500"></div>
            <div className="h-3 w-3 rounded-full bg-green-500"></div>
          </div>
          <div className="text-sm font-medium text-center flex-1">
            SQLPilot Terminal
          </div>
        </div>

        <div className="flex-1 bg-dark-300/80 p-4 font-mono text-sm overflow-auto custom-scrollbar">
          {/* Terminal Welcome */}
          <motion.div variants={item} className="text-neon-blue mb-4">
            Welcome to SQLPilot AI Assistant. How can I help you today?
          </motion.div>

          {/* Natural Language Input */}
          {currentStep >= 1 && (
            <motion.div
              variants={item}
              className="mb-6"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
            >
              <div className="text-neon-pink mb-1">$ describe schema</div>
              <div className="bg-dark-200 rounded-md p-3 border border-white/10">
                {isTyping && currentStep === 1 ? (
                  <div className="flex">
                    <motion.div
                      className="typing-effect"
                      initial={{ width: "0%" }}
                      animate={{ width: "100%" }}
                      transition={{ duration: 3 }}
                    >
                      {naturalLanguageInput}
                    </motion.div>
                    <motion.span
                      variants={typingCursor}
                      initial="hidden"
                      animate="visible"
                      className="ml-0.5 border-r-2 border-neon-purple h-5"
                    ></motion.span>
                  </div>
                ) : (
                  <div>{naturalLanguageInput}</div>
                )}
              </div>
            </motion.div>
          )}

          {/* Generated Schema */}
          {currentStep >= 2 && (
            <motion.div
              variants={item}
              className="mb-6"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
            >
              <div className="text-neon-teal mb-1">$ generating schema...</div>
              <div className="bg-dark-200 rounded-md p-3 border border-white/10 overflow-x-auto">
                <pre className="text-neon-teal">{generatedSchema}</pre>
              </div>
              <div className="text-green-400 mt-1">
                ✓ Schema generated successfully
              </div>
            </motion.div>
          )}

          {/* Natural Language Query */}
          {currentStep >= 3 && (
            <motion.div
              variants={item}
              className="mb-6"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
            >
              <div className="text-neon-pink mb-1">$ ask query</div>
              <div className="bg-dark-200 rounded-md p-3 border border-white/10">
                {isTyping && currentStep === 3 ? (
                  <div className="flex">
                    <motion.div
                      className="typing-effect"
                      initial={{ width: "0%" }}
                      animate={{ width: "100%" }}
                      transition={{ duration: 3 }}
                    >
                      {naturalLanguageQuery}
                    </motion.div>
                    <motion.span
                      variants={typingCursor}
                      initial="hidden"
                      animate="visible"
                      className="ml-0.5 border-r-2 border-neon-purple h-5"
                    ></motion.span>
                  </div>
                ) : (
                  <div>{naturalLanguageQuery}</div>
                )}
              </div>
            </motion.div>
          )}

          {/* Generated SQL */}
          {currentStep >= 4 && (
            <motion.div
              variants={item}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ duration: 0.5 }}
            >
              <div className="text-neon-teal mb-1">
                $ generating SQL (Trino)...
              </div>
              <div className="bg-dark-200 rounded-md p-3 border border-white/10 overflow-x-auto">
                <pre className="text-neon-blue">{generatedSQL}</pre>
              </div>
              <div className="text-green-400 mt-1">
                ✓ SQL query generated successfully
              </div>
              <div className="mt-2 text-neon-yellow">$ executing query...</div>
              <motion.div
                className="mt-2 flex items-center gap-2"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 1, duration: 0.5 }}
              >
                <div className="text-green-400">
                  ✓ Query executed successfully (5 rows returned)
                </div>
                <motion.div
                  className="text-xs text-neon-pink cursor-pointer"
                  whileHover={{ scale: 1.05 }}
                >
                  [View Results]
                </motion.div>
              </motion.div>
            </motion.div>
          )}

          {/* Typing Cursor */}
          {isTyping && currentStep < 4 && (
            <motion.div
              variants={typingCursor}
              initial="hidden"
              animate="visible"
              className="border-r-2 border-neon-purple h-5"
            ></motion.div>
          )}
        </div>
      </motion.div>
    </div>
  );
}
