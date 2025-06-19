document
  .getElementById("recommendForm")
  .addEventListener("submit", async function (e) {
    e.preventDefault();
    document.getElementById("errorSection").style.display = "none";
    document.getElementById("resultSection").style.display = "none";
    document.getElementById("recommendations").innerHTML = "";

    const query = document.getElementById("query").value.trim();
    const top_n = parseInt(document.getElementById("top_n").value) || 5;

    if (!query) {
      showError("Please enter a job query.");
      return;
    }

    try {
      const response = await fetch("/recommend/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ query, top_n }),
      });
      const data = await response.json();
      if (!response.ok) {
        showError(data.error || "An error occurred.");
        return;
      }
      showRecommendations(data.recommendations);
    } catch (err) {
      showError(
        "Failed to connect to the server. Make sure the backend is running."
      );
    }
  });

function showRecommendations(recommendations) {
  const container = document.getElementById("recommendations");
  if (!recommendations || recommendations.length === 0) {
    container.innerHTML =
      '<div class="alert alert-warning">No recommendations found.</div>';
  } else {
    container.innerHTML = recommendations
      .map(
        (job) => `
            <div class="card-job">
                <div class="job-title">${job.title} <span class="score">(Score: ${job.similarity_score})</span></div>
                <div class="company">${job.company} - ${job.location} (${job.country})</div>
                <div><strong>Type:</strong> ${job.work_type} | <strong>Salary:</strong> ${job.salary}</div>
                <div><strong>Experience:</strong> ${job.experience} | <strong>Preference:</strong> ${job.preference}</div>
                <div><strong>Skills:</strong> ${job.skills}</div>
                <div><strong>Contact:</strong> ${job.contact} (${job.contact_person})</div>
                <div><strong>Qualification:</strong> ${job.qualification}</div>
                <div class="mt-2"><strong>Description:</strong> ${job.desc}</div>
            </div>
        `
      )
      .join("");
  }
  document.getElementById("resultSection").style.display = "block";
}

function showError(msg) {
  const errorDiv = document.getElementById("errorSection");
  errorDiv.textContent = msg;
  errorDiv.style.display = "block";
}
