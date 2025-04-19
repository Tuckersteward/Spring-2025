const imagesDirectory = "images/portfolio";

document.addEventListener("DOMContentLoaded", () => {
    const imageModal = document.getElementById("imageModal");
    const modalImage = document.getElementById("modalImage");
    const closeBtn = document.querySelector(".close");
    const prevBtn = document.getElementById("prevBtn");
    const nextBtn = document.getElementById("nextBtn");

    const imageList = [
        { name: "image_1.jpg", category: "Lifestyles" },
        { name: "image_2.jpg", category: "Landscapes" },
        { name: "image_3.jpg", category: "Landscapes" },
        { name: "image_4.jpg", category: "Lifestyles" },
        { name: "image_5.jpg", category: "Landscapes" },
        { name: "image_6.jpg", category: "Landscapes" },
        { name: "image_7.jpg", category: "Lifestyles" },
        { name: "image_8.jpg", category: "Sports" },
        { name: "image_9.jpg", category: "Sports" },
        { name: "image_10.jpg", category: "Sports" },
        { name: "image_11.jpg", category: "Sports" },
        { name: "image_12.jpg", category: "Landscapes" },
        { name: "image_13.jpg", category: "Landscapes" },
        { name: "image_15.jpg", category: "Sports" },
        { name: "image_16.jpg", category: "Sports" },
        { name: "image_17.jpg", category: "Sports" },
        { name: "image_18.jpg", category: "Sports" },
        { name: "image_19.jpg", category: "Sports" },
        { name: "image_20.jpg", category: "Lifestyles" },
        { name: "image_21.jpg", category: "Sports" },
        { name: "image_22.jpg", category: "Landscapes" },
        { name: "Exi-Graduation-37.jpg", category: "Portraits" },
        { name: "Exi-Graduation-105.jpg", category: "Portraits" },
        { name: "Exi-Graduation-109.jpg", category: "Portraits" },
        { name: "Moab-Climbing-44.jpg", category: "Sports" }

    ];

    const grids = {
        Portraits: document.getElementById("gridPortraits"),
        Landscapes: document.getElementById("gridLandscapes"),
        Sports: document.getElementById("gridSports"),
        Lifestyles: document.getElementById("gridLifestyles")
    };

    let currentIndex = null;

    imageModal.style.display = "none";

    imageList.forEach((imageObj, index) => {
        const imgElement = document.createElement("img");
        imgElement.src = `${imagesDirectory}/${imageObj.name}`;
        imgElement.alt = "Portfolio Image";
        imgElement.dataset.index = index;
        imgElement.addEventListener("click", () => openModal(index));

        const targetGrid = grids[imageObj.category];
        if (targetGrid) {
            targetGrid.appendChild(imgElement);
        }
    });

    function openModal(index) {
        currentIndex = index;
        modalImage.src = `${imagesDirectory}/${imageList[index].name}`;
        imageModal.style.display = "flex";
    }

    closeBtn.addEventListener("click", () => {
        imageModal.style.display = "none";
        currentIndex = null;
    });

    prevBtn.addEventListener("click", () => {
        if (currentIndex !== null) {
            currentIndex = (currentIndex - 1 + imageList.length) % imageList.length;
            modalImage.src = `${imagesDirectory}/${imageList[currentIndex].name}`;
        }
    });

    nextBtn.addEventListener("click", () => {
        if (currentIndex !== null) {
            currentIndex = (currentIndex + 1) % imageList.length;
            modalImage.src = `${imagesDirectory}/${imageList[currentIndex].name}`;
        }
    });

    imageModal.addEventListener("click", (e) => {
        if (e.target === imageModal) {
            imageModal.style.display = "none";
            currentIndex = null;
        }
    });
});
