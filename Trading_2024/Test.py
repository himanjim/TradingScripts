"""
make_bra_doc_with_14_figures.py

What this does
- Takes:
  A) AI component Sharma Himanshu_VSI_manuscript_min_IEEE.docx
  B) Blockchain_component_2025_12_18_CONSOLIDATED.docx
  C) Sharma_InternetVoting_BlockchainResearchApplications_TextOnly.docx
- Extracts ALL images from A and B (typically 7 + 7 = 14)
- Inserts them into C exactly per your 14-figure insertion plan:
  Figure 1..14 with:
    - required in-text mentions (exact wording from your plan)
    - captions "Figure n. ..."
    - center alignment, inline images, blank line after caption
- Outputs: Sharma_InternetVoting_BRA_With14Figures.docx

Requirements
  pip install python-docx pillow
"""

import os
import re
import zipfile
import hashlib
from dataclasses import dataclass
from typing import List, Optional, Dict

from PIL import Image
from docx import Document
from docx.shared import Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.text.paragraph import Paragraph


# =========================
# CONFIG: set your paths
# =========================
A_DOC = r"G:\My Drive\PhD\My Research papers\Master documents\AI component Sharma Himanshu_VSI_manuscript_min_IEEE.docx"
B_DOC = r"G:\My Drive\PhD\My Research papers\Master documents\Blockchain_component_2025_12_18_CONSOLIDATED.docx"
C_DOC = r"C:\Users\himan\Downloads\Sharma_InternetVoting_BlockchainResearchApplications_TextOnly.docx"
OUT_DOC = r"C:\Users\himan\Downloads\Sharma_InternetVoting_BlockchainResearchApplications_Images.docx"

WORKDIR = r"_tmp_extracted_figs"  # temp folder


# =========================
# Robust text matching
# =========================
def _as_text(x):
    return x.text if hasattr(x, "text") else ("" if x is None else str(x))


def _norm(s: str) -> str:
    s = _as_text(s)
    s = s.replace("\xa0", " ")                 # NBSP -> space
    s = re.sub(r"\s+", " ", s).strip()         # collapse whitespace
    return s.casefold()


def find_para(
    doc: Document,
    *,
    equals: Optional[str] = None,
    startswith: Optional[str] = None,
    contains: Optional[str] = None,
    regex: Optional[str] = None,
    start: int = 0,
) -> Optional[Paragraph]:
    """
    Robust paragraph finder.
    - equals/startswith/contains are matched after normalization
    - regex is applied on the raw paragraph text (IGNORECASE)
    """
    paras = doc.paragraphs
    for i in range(start, len(paras)):
        raw = paras[i].text
        t = _norm(raw)

        if equals is not None and t == _norm(equals):
            return paras[i]
        if startswith is not None and t.startswith(_norm(startswith)):
            return paras[i]
        if contains is not None and _norm(contains) in t:
            return paras[i]
        if regex is not None and re.search(regex, raw, flags=re.IGNORECASE):
            return paras[i]
    return None


def require(p: Optional[Paragraph], what: str) -> Paragraph:
    if p is None:
        raise ValueError(f"Could not find required paragraph for: {what}")
    return p


def append_sentence(paragraph: Optional[Paragraph], sentence: str) -> None:
    paragraph = require(paragraph, f"append_sentence -> {sentence[:80]}...")
    t = paragraph.text.rstrip()
    if t and not t.endswith((".", "!", "?")):
        paragraph.add_run(".")
    paragraph.add_run((" " + sentence) if t else sentence)


# =========================
# Paragraph insertion helpers
# =========================
def insert_paragraph_after(paragraph: Paragraph, text: Optional[str] = None) -> Paragraph:
    new_p = OxmlElement("w:p")
    paragraph._p.addnext(new_p)
    new_para = Paragraph(new_p, paragraph._parent)
    if text is not None:
        new_para.add_run(text)
    return new_para


def first_nonempty_paragraph_after_heading(doc: Document, heading_para: Paragraph) -> Paragraph:
    """
    Finds the first non-empty paragraph after heading_para in the *current* doc.
    (Safe even after insertions.)
    """
    paras = doc.paragraphs
    # Find by identity (best-effort); fallback to searching by text match if needed.
    idx = None
    for i, p in enumerate(paras):
        if p._p is heading_para._p:
            idx = i
            break
    if idx is None:
        # fallback: find the first paragraph whose text matches heading text exactly normalized
        ht = _norm(heading_para.text)
        for i, p in enumerate(paras):
            if _norm(p.text) == ht:
                idx = i
                break
    if idx is None:
        raise ValueError(f"Could not locate heading paragraph again: {heading_para.text!r}")

    for j in range(idx + 1, len(paras)):
        if paras[j].text.strip():
            return paras[j]
    raise ValueError(f"No non-empty paragraph found after heading: {heading_para.text!r}")


# =========================
# Image extraction
# =========================
@dataclass
class ExtractedImage:
    path: str
    size_bytes: int


def extract_media(docx_path: str, prefix: str) -> List[ExtractedImage]:
    """
    Extracts word/media/image* in document order (image1, image2, ...).
    Returns list of ExtractedImage.
    """
    os.makedirs(WORKDIR, exist_ok=True)

    with zipfile.ZipFile(docx_path, "r") as z:
        media = [n for n in z.namelist() if n.startswith("word/media/") and not n.endswith("/")]

        def keyfn(n: str):
            m = re.search(r"image(\d+)", n)
            return int(m.group(1)) if m else 999999

        media = sorted(media, key=keyfn)

        out: List[ExtractedImage] = []
        for i, name in enumerate(media, start=1):
            data = z.read(name)
            ext = os.path.splitext(name)[1].lower()
            h = hashlib.sha1(data).hexdigest()[:10]
            out_path = os.path.join(WORKDIR, f"{prefix}_{i:02d}_{h}{ext}")
            with open(out_path, "wb") as f:
                f.write(data)
            out.append(ExtractedImage(path=out_path, size_bytes=len(data)))
        return out


def width_for_image(path: str) -> Inches:
    # Keep within margins (Word A4/Letter typical). Wider diagrams get larger width.
    with Image.open(path) as im:
        w, h = im.size
    aspect = w / max(h, 1)
    if aspect >= 2.0:
        return Inches(6.6)
    elif aspect >= 1.3:
        return Inches(6.2)
    else:
        return Inches(5.6)


def insert_figure_after(paragraph: Paragraph, fig_no: int, img_path: str, caption: str) -> Paragraph:
    img_p = insert_paragraph_after(paragraph)
    img_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = img_p.add_run()
    run.add_picture(img_path, width=width_for_image(img_path))

    cap_p = insert_paragraph_after(img_p, caption)
    cap_p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    blank = insert_paragraph_after(cap_p, "")  # blank line after caption
    return blank


# =========================
# Main build
# =========================
def main():
    # Validate input paths
    for p in (A_DOC, B_DOC, C_DOC):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing file: {p}")

    # Extract images
    a_imgs = extract_media(A_DOC, "A")  # usually 7
    b_imgs = extract_media(B_DOC, "B")  # usually 7

    if len(a_imgs) < 7 or len(b_imgs) < 7:
        raise ValueError(
            f"Expected at least 7 images each in A and B. Got A={len(a_imgs)}, B={len(b_imgs)}.\n"
            f"Tip: open {WORKDIR} and verify extracted images."
        )

    # Default mapping (doc-order based) for your 14-figure plan
    # A provides: 1,2,3,5,9,10,14  -> A[0],A[1],A[2],A[4],A[3],A[5],A[6]
    # B provides: 4,6,7,8,11,12,13 -> B[0],B[1],B[2],B[5],B[3],B[4],B[6]
    fig_img: Dict[int, str] = {
        1: a_imgs[0].path,
        2: a_imgs[1].path,
        3: a_imgs[2].path,
        4: b_imgs[0].path,
        5: a_imgs[4].path,
        6: b_imgs[1].path,
        7: b_imgs[2].path,
        8: b_imgs[5].path,
        9: a_imgs[3].path,
        10: a_imgs[5].path,
        11: b_imgs[3].path,
        12: b_imgs[4].path,
        13: b_imgs[6].path,
        14: a_imgs[6].path,
    }

    # Captions EXACTLY per your pasted plan
    captions = {
        1: "Figure 1. End-to-end system architecture for assisted internet voting, combining biometric verification and a permissioned blockchain core under multi-trustee governance.",
        2: "Figure 2. Assisted kiosk voting workflow showing biometric verification, ballot casting, receipt generation, and re-voting support.",
        3: "Figure 3. Remote personal-device voting flow illustrating the additional client-integrity and coercion risks relative to assisted endpoints.",
        4: "Figure 4. Three-trustee governance model implemented on a permissioned Fabric network, enforcing checks-and-balances through endorsement and access control.",
        5: "Figure 5. Prototype Fabric network topology used in evaluation, showing trustee organizations, peers, ordering service, and channel participation.",
        6: "Figure 6. Audit and dispute-resolution workflow using on-chain integrity anchors, private data boundaries, and event-driven operational evidence.",
        7: "Figure 7. QR receipt construction and verification flow for inclusion and supersession, designed to avoid transferable proof of vote choice.",
        8: "Figure 8. Result publication and key-custody model supporting aggregate-only decryption while preventing recovery of individual vote choices.",
        9: "Figure 9. Production-style biometric verification and de-duplication stack with separated inference and vector search services for predictable tail latency.",
        10: "Figure 10. Illustrative identity artifact motivating election-scoped identifiers and strict separation of civil identity from ballot records.",
        11: "Figure 11. Benchmark summary of vote-casting throughput and latency distribution under the baseline configuration.",
        12: "Figure 12. Tail-latency behavior under high concurrency, emphasizing p95/p99 operational implications.",
        13: "Figure 13. Comparative benchmark outcomes for Raft versus SmartBFT under identical application semantics.",
        14: "Figure 14. Conceptual illustration of the intended voter interaction model for assisted internet voting.",
    }

    # In-text mentions EXACTLY per your plan
    mentions = {
        1: "Figure 1 summarizes the end-to-end architecture and the division of responsibilities across the identity, AI, client, and blockchain layers.",
        2: "Figure 2 outlines the assisted kiosk workflow, including biometric gating, ballot submission, receipt issuance, and re-voting.",
        3: "Figure 3 contrasts a remote personal-device flow and highlights the stronger client-integrity assumptions such a mode would require.",
        4: "Figure 4 depicts the three-trustee governance model as instantiated through authenticated MSP identities, endorsement rules, and privacy partitioning.",
        5: "Figure 5 shows the benchmark network topology used to evaluate vote casting and election lifecycle operations under the trustee model.",
        6: "Figure 6 summarizes the audit and dispute-resolution workflow enabled by immutable ledger history, private collections, and structured events.",
        7: "Figure 7 illustrates how the QR receipt binds to committed ledger state and supports inclusion and supersession checks without revealing vote choice.",
        8: "Figure 8 presents the aggregate-only decryption concept and key-custody separation required to prevent decryption of individual ballots.",
        9: "Figure 9 shows the deployable inference-and-search stack used for biometric verification and large-scale de-duplication.",
        10: "Figure 10 provides an illustrative identity artifact motivating election-scoped serials and strict separation between civil identity and voting records.",
        11: "Figure 11 summarizes the primary throughput and latency distribution observed under the baseline ordering configuration.",
        12: "Figure 12 highlights tail-latency behavior under high concurrency, which determines queueing at peak booth load.",
        13: "Figure 13 contrasts Raft and SmartBFT outcomes under the same application workload and endorsement regime.",
        14: "Figure 14 provides a high-level conceptual illustration of the intended voter interaction model.",
    }

    doc = Document(C_DOC)

    # -----------------------------
    # Figure 1: after exact paragraph
    # -----------------------------
    p = require(find_para(doc, equals="The end-to-end system comprises five components."),
                "Figure 1 insertion anchor: 'The end-to-end system comprises five components.'")
    append_sentence(p, mentions[1])
    insert_figure_after(p, 1, fig_img[1], captions[1])

    # -----------------------------------
    # Figure 2: after workflow paragraph
    # Plan says after paragraph that starts “Workflow overview.”
    # In merged C, the workflow paragraph begins “A voter presents at a managed voting endpoint.”
    # -----------------------------------
    p = require(find_para(doc, startswith="A voter presents at a managed voting endpoint."),
                "Figure 2 insertion anchor: workflow paragraph")
    append_sentence(p, mentions[2])
    blank_after_fig2 = insert_figure_after(p, 2, fig_img[2], captions[2])

    # -----------------------------------
    # Figure 3: immediately after Figure 2,
    # right after paragraph explaining unsupervised personal-device not claimed.
    # If such paragraph isn't present, we insert one right after Figure 2 block.
    # -----------------------------------
    scope_para = insert_paragraph_after(
        blank_after_fig2,
        "The design does not claim to solve fully unsupervised personal-device internet voting. "
        "Remote casting on arbitrary devices increases exposure to malware, coercion, and unverifiable client integrity in ways that the governance layer cannot fully compensate for. "
        + mentions[3]
    )
    insert_figure_after(scope_para, 3, fig_img[3], captions[3])

    # -----------------------------------
    # Figure 4: after opening paragraph in Blockchain Module
    # starts “Hyperledger Fabric provides the permissioning, policy, and audit substrate…”
    # -----------------------------------
    p = require(find_para(doc, startswith="Hyperledger Fabric provides the permissioning, policy, and audit substrate"),
                "Figure 4 insertion anchor: Blockchain Module opening paragraph")
    append_sentence(p, mentions[4])
    insert_figure_after(p, 4, fig_img[4], captions[4])

    # -----------------------------------
    # Figure 5: under “Network and channel topology”
    # after the first paragraph after that heading
    # -----------------------------------
    head = require(find_para(doc, equals="Network and channel topology."),
                   "Figure 5 heading: 'Network and channel topology.'")
    p = first_nonempty_paragraph_after_heading(doc, head)
    append_sentence(p, mentions[5])
    insert_figure_after(p, 5, fig_img[5], captions[5])

    # -----------------------------------
    # Figure 6: after paragraph starting “Audit-grade metadata without turning the ledger into a surveillance log.”
    # -----------------------------------
    p = require(find_para(doc, startswith="Audit-grade metadata without turning the ledger into a surveillance log."),
                "Figure 6 anchor: audit-grade metadata paragraph")
    append_sentence(p, mentions[6])
    insert_figure_after(p, 6, fig_img[6], captions[6])

    # -----------------------------------
    # Figure 7: under “Receipt binding and voter-side verifiability.”
    # right after you describe what the QR contains (we place after first paragraph after heading)
    # -----------------------------------
    head = require(find_para(doc, equals="Receipt binding and voter-side verifiability."),
                   "Figure 7 heading: 'Receipt binding and voter-side verifiability.'")
    p = first_nonempty_paragraph_after_heading(doc, head)
    append_sentence(p, mentions[7])
    insert_figure_after(p, 7, fig_img[7], captions[7])

    # -----------------------------------
    # Figure 8: under “Encrypted tallying and aggregate-only decryption.”
    # insert after the paragraph ending with point: individual ballots confidential, only aggregates decrypted
    # We locate heading and then choose first substantive paragraph after it.
    # -----------------------------------
    head = require(find_para(doc, equals="Encrypted tallying and aggregate-only decryption."),
                   "Figure 8 heading: 'Encrypted tallying and aggregate-only decryption.'")
    p = first_nonempty_paragraph_after_heading(doc, head)
    append_sentence(p, mentions[8])
    insert_figure_after(p, 8, fig_img[8], captions[8])

    # -----------------------------------
    # Figure 9: AI Module after paragraph “Vector search service for de-duplication.”
    # -----------------------------------
    p = require(find_para(doc, startswith="Vector search service for de-duplication."),
                "Figure 9 anchor: 'Vector search service for de-duplication.'")
    append_sentence(p, mentions[9])
    insert_figure_after(p, 9, fig_img[9], captions[9])

    # -----------------------------------
    # Figure 10: AI Module under “Privacy-by-design posture.”
    # Insert after the paragraph where you say embeddings/templates are sensitive and raw images minimized.
    # In merged C, that paragraph starts “Biometrics create persistent identifiers…”
    # -----------------------------------
    p = require(find_para(doc, startswith="Biometrics create persistent identifiers"),
                "Figure 10 anchor: privacy-by-design paragraph")
    append_sentence(p, mentions[10])
    insert_figure_after(p, 10, fig_img[10], captions[10])

    # -----------------------------------
    # Figure 11: Implementation and Evaluation after “Results and operational interpretation.”
    # -----------------------------------
    p = require(find_para(doc, startswith="Results and operational interpretation."),
                "Figure 11 anchor: 'Results and operational interpretation.'")
    append_sentence(p, mentions[11])
    insert_figure_after(p, 11, fig_img[11], captions[11])

    # -----------------------------------
    # Figure 12: after paragraph discussing tail latency (p95/p99)
    # -----------------------------------
    p = find_para(doc, contains="p95") if find_para(doc, contains="p95") else find_para(doc, contains="tail latency")
    p = require(p, "Figure 12 anchor: tail latency paragraph (p95/p99 or 'tail latency')")
    append_sentence(p, mentions[12])
    insert_figure_after(p, 12, fig_img[12], captions[12])

    # -----------------------------------
    # Figure 13: after paragraph comparing Raft vs SmartBFT
    # Plan says after the paragraph where you compare Raft vs SmartBFT.
    # In merged C there is a paragraph starting “Raft versus SmartBFT choice.”
    # -----------------------------------
    p = find_para(doc, startswith="Raft versus SmartBFT choice.") or find_para(doc, regex=r"Raft.*SmartBFT")
    p = require(p, "Figure 13 anchor: Raft vs SmartBFT paragraph")
    append_sentence(p, mentions[13])
    insert_figure_after(p, 13, fig_img[13], captions[13])

    # -----------------------------------
    # Figure 14: Discussion section after paragraph about usability/inclusivity and voter experience.
    # We anchor on “Biometric risks and inclusivity.”
    # -----------------------------------
    p = require(find_para(doc, startswith="Biometric risks and inclusivity."),
                "Figure 14 anchor: 'Biometric risks and inclusivity.'")
    append_sentence(p, mentions[14])
    insert_figure_after(p, 14, fig_img[14], captions[14])

    doc.save(OUT_DOC)
    print(f"OK: wrote {OUT_DOC}")
    print(f"Extracted images are in: {os.path.abspath(WORKDIR)}")
    print("If any figure-image mapping looks swapped, adjust the fig_img mapping block near the top.")


if __name__ == "__main__":
    main()
