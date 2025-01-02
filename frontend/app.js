document.getElementById("upload-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const fileInput = document.getElementById("csv-file");
    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetch("http://127.0.0.1:8000/upload_csv/", {
            method: "POST",
            body: formData,
        });
        const result = await response.json();
        console.log("File uploaded:", result);
        alert(`File uploaded: ${result.filename}`);
    } catch (error) {
        console.error("Error uploading file:", error);
    }
});

document.getElementById("push-tree").addEventListener("click", async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/push_tree/", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: new URLSearchParams({ tree_name: "Test Tree" }),
        });
        const result = await response.json();
        alert(result.message);
    } catch (error) {
        console.error("Error pushing tree:", error);
    }
});

fetch("http://127.0.0.1:8000/process-csv/", {
    method: "POST",
    body: formData, // Include the CSV file here
  })
    .then((response) => response.json())
    .then((data) => console.log(data))
    .catch((error) => console.error("Error:", error));

document.addEventListener("DOMContentLoaded", async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/visualize_tree/");
        const data = await response.json();
        const treeVisualization = document.getElementById("tree-visualization");
        treeVisualization.innerHTML = `<pre>${JSON.stringify(data.tree_structure, null, 2)}</pre>`;
    } catch (error) {
        console.error("Error fetching tree visualization:", error);
    }
});

document.getElementById("create-lookup-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const groupColumn = document.getElementById("group-column").value;
    const keyColumn = document.getElementById("key-column").value;
    const valueColumn = document.getElementById("value-column").value;

    try {
        const response = await fetch("http://127.0.0.1:8000/create_lookup/", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: new URLSearchParams({
                group_column: groupColumn,
                key_column: keyColumn,
                value_column: valueColumn,
            }),
        });
        const result = await response.json();
        alert(result.message);
    } catch (error) {
        console.error("Error creating lookup:", error);
    }
});

// Visualize Tree button handler
document.getElementById("visualize-tree").addEventListener("click", async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/visualize_tree/", {
            method: "GET",
        });
        const result = await response.json();
        console.log("Tree visualization:", result);
        
        // Display the tree structure in the "tree-visualization" div
        const visualizationDiv = document.getElementById("tree-visualization");
        visualizationDiv.innerText = JSON.stringify(result.tree_structure, null, 2);
    } catch (error) {
        console.error("Error visualizing tree:", error);
    }
});

document.getElementById("upload-form").addEventListener("submit", async (event) => {
    event.preventDefault(); // Prevent default form submission behavior
    const fileInput = document.getElementById("csv-file");
    const formData = new FormData();
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetch("http://127.0.0.1:8000/upload_csv/", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        console.log("File uploaded:", result);
        alert(`File uploaded: ${result.filename}`);
    } catch (error) {
        console.error("Error uploading file:", error);
    }
});

@app.post("/upload_csv/")
async def upload_csv(file: UploadFile):
    """
    Endpoint to upload a CSV file.
    """
    try:
        file_location = f"./uploaded_files/{file.filename}"
        os.makedirs("./uploaded_files", exist_ok=True)  # Ensure the directory exists

        # Save the uploaded file
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Example: Process the file and return column names
        data = pd.read_csv(file_location)
        return {"filename": file.filename, "columns": list(data.columns)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")