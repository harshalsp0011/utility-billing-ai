import streamlit as st
from pathlib import Path
from datetime import datetime
import pandas as pd

from src.database.db_utils import insert_raw_bill_document
from src.agents.document_processor_agent.utility_bill_doc_processor import process_bill


def render_file_uploader():
    st.title("📁 File Upload Management")

    # Tab navigation for separate sections
    tab1, tab2 = st.tabs(["📄 Bill Documents", "⚡ Tariff Documents"])

    # -----------------------------
    # TAB 1: Bill Upload
    # -----------------------------
    with tab1:
        st.subheader("Upload Bill Files")
        st.caption("Upload monthly or quarterly billing statements (PDF only).")

        bill_file = st.file_uploader(
            "Select billing file",
            type=["pdf"],
            accept_multiple_files=False,
            key="bill_uploader"
        )

        if bill_file:
            save_dir = Path("data/raw")
            save_dir.mkdir(parents=True, exist_ok=True)

            file = bill_file
            file_path = save_dir / file.name
            file_path.write_bytes(file.read())

            # Log upload in DB
            metadata = {
                "file_name": file.name,
                "file_type": Path(file.name).suffix.lower(),
                "upload_date": datetime.utcnow(),
                "source": "User Upload (Bill)",
                "status": "uploaded"
            }

            try:
                insert_raw_bill_document(metadata)
            except Exception as e:
                st.error(f"Error logging bill file {file.name}: {e}")

            # -------------------------
            # 🔥 AUTO-PROCESS THE FILE
            # -------------------------
            try:
                # Create a full-page modal overlay
                st.markdown("""
                    <style>
                    .stApp {
                        pointer-events: none;
                    }
                    div[data-testid="stAppViewContainer"] > section {
                        filter: blur(5px);
                    }
                    section[data-testid="stSidebar"] {
                        pointer-events: none;
                        filter: blur(5px);
                    }
                    </style>
                """, unsafe_allow_html=True)
                
                processing_placeholder = st.empty()
                
                with processing_placeholder.container():
                    st.markdown("""
                        <div style='position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
                             background: rgba(0, 0, 0, 0.7); backdrop-filter: blur(8px);
                             z-index: 9999; display: flex; align-items: center; justify-content: center;
                             pointer-events: all;'>
                            <div style='background: white; padding: 40px; border-radius: 10px; 
                                 text-align: center; box-shadow: 0 4px 20px rgba(0,0,0,0.3);'>
                                <h2 style='color: #1f77b4; margin-bottom: 20px;'>🔄 Processing Bill Document</h2>
                                <p style='font-size: 18px; font-weight: bold; color: #333; margin-bottom: 10px;'>{}</p>
                                <p style='color: #666; margin-bottom: 20px;'>Please wait while we extract and validate the billing data...</p>
                                <div style='width: 100%; height: 4px; background: #e0e0e0; border-radius: 2px; overflow: hidden;'>
                                    <div style='width: 50%; height: 100%; background: linear-gradient(90deg, #1f77b4, #4fc3f7); 
                                         animation: loading 1.5s ease-in-out infinite;'></div>
                                </div>
                            </div>
                        </div>
                        <style>
                        @keyframes loading {{
                            0% {{ transform: translateX(-100%); }}
                            50% {{ transform: translateX(100%); }}
                            100% {{ transform: translateX(-100%); }}
                        }}
                        </style>
                    """.format(file.name), unsafe_allow_html=True)
                
                # Process the file
                df, total_anomalies = process_bill(file_path)
                
                # Clear the processing overlay and re-enable page
                processing_placeholder.empty()
                st.markdown("""
                    <style>
                    .stApp {
                        pointer-events: auto;
                    }
                    div[data-testid="stAppViewContainer"] > section {
                        filter: none;
                    }
                    section[data-testid="stSidebar"] {
                        pointer-events: auto;
                        filter: none;
                    }
                    </style>
                """, unsafe_allow_html=True)

                # Display results in a clean card layout
                st.markdown(f"### 📄 {file.name}")
                
                # Anomalies metric with tip on the right
                col1, col2 = st.columns([1, 3])
                with col1:
                    st.metric(label="Anomalies detected", value=int(total_anomalies))
                with col2:
                    st.info("💡 Tip: check Audit Bills section to get better insights.")
                
                # Data table with index starting from 1
                df_display = df.copy()
                df_display.index = df_display.index + 1
                st.dataframe(df_display, width='stretch')

            except Exception as e:
                st.error(f"❌ Failed to process {file.name}: {e}")

    # -----------------------------
    # TAB 2: Tariff Upload
    # -----------------------------
    with tab2:
        st.subheader("Upload Tariff Documents")
        st.caption("Upload the latest tariff document for your utility provider (PDF only).")

        tariff_files = st.file_uploader(
            "Select tariff files",
            type=["pdf"],
            accept_multiple_files=True,
            key="tariff_uploader"
        )

        if tariff_files:
            tariff_dir = Path("data/raw/tariff")
            tariff_dir.mkdir(parents=True, exist_ok=True)

            for file in tariff_files:
                try:
                    # Create a full-page modal overlay
                    st.markdown("""
                        <style>
                        .stApp {
                            pointer-events: none;
                        }
                        div[data-testid="stAppViewContainer"] > section {
                            filter: blur(5px);
                        }
                        section[data-testid="stSidebar"] {
                            pointer-events: none;
                            filter: blur(5px);
                        }
                        </style>
                    """, unsafe_allow_html=True)
                    
                    processing_placeholder = st.empty()
                    
                    with processing_placeholder.container():
                        st.markdown("""
                            <div style='position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; 
                                 background: rgba(0, 0, 0, 0.7); backdrop-filter: blur(8px);
                                 z-index: 9999; display: flex; align-items: center; justify-content: center;
                                 pointer-events: all;'>
                                <div style='background: white; padding: 40px; border-radius: 10px; 
                                     text-align: center; box-shadow: 0 4px 20px rgba(0,0,0,0.3);'>
                                    <h2 style='color: #1f77b4; margin-bottom: 20px;'>🔄 Processing Tariff Document</h2>
                                    <p style='font-size: 18px; font-weight: bold; color: #333; margin-bottom: 10px;'>{}</p>
                                    <p style='color: #666; margin-bottom: 20px;'>Please wait while we upload and process the tariff data...</p>
                                    <div style='width: 100%; height: 4px; background: #e0e0e0; border-radius: 2px; overflow: hidden;'>
                                        <div style='width: 50%; height: 100%; background: linear-gradient(90deg, #1f77b4, #4fc3f7); 
                                             animation: loading 1.5s ease-in-out infinite;'></div>
                                    </div>
                                </div>
                            </div>
                            <style>
                            @keyframes loading {{
                                0% {{ transform: translateX(-100%); }}
                                50% {{ transform: translateX(100%); }}
                                100% {{ transform: translateX(-100%); }}
                            }}
                            </style>
                        """.format(file.name), unsafe_allow_html=True)
                    
                    # Process the file
                    file_path = tariff_dir / file.name
                    file_path.write_bytes(file.read())

                    metadata = {
                        "file_name": file.name,
                        "file_type": Path(file.name).suffix.lower(),
                        "upload_date": datetime.utcnow(),
                        "source": "User Upload (Tariff)",
                        "status": "uploaded"
                    }

                    insert_raw_bill_document(metadata)
                    
                    # Clear the processing overlay and re-enable page
                    processing_placeholder.empty()
                    st.markdown("""
                        <style>
                        .stApp {
                            pointer-events: auto;
                        }
                        div[data-testid="stAppViewContainer"] > section {
                            filter: none;
                        }
                        section[data-testid="stSidebar"] {
                            pointer-events: auto;
                            filter: none;
                        }
                        </style>
                    """, unsafe_allow_html=True)
                    
                    st.success(f"✅ Tariff file uploaded and logged: {file.name}")
                    
                except Exception as e:
                    st.error(f"❌ Error logging tariff file {file.name}: {e}")
            
            st.info("💡 Tip: After uploading tariff documents, navigate to the Tariff Analysis section to extract and analyze rate structures.")
